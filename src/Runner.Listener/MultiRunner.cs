using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GitHub.DistributedTask.WebApi;
using GitHub.Runner.Common;
using GitHub.Runner.Common.Util;
using GitHub.Runner.Listener.Configuration;
using GitHub.Runner.Sdk;
using GitHub.Services.WebApi;
using Pipelines = GitHub.DistributedTask.Pipelines;

namespace GitHub.Runner.Listener
{
    [ServiceLocator(Default = typeof(MultiRunnerCoordinator))]
    public interface IMultiRunnerCoordinator : IRunnerService
    {
        Task<int> RunAsync();
    }

    // Supervises one message listener per configured repository registration.
    // All registrations long-poll concurrently; when a job arrives from one,
    // the other registrations' sessions are deleted so their runners show
    // Offline in GitHub (letting other machines' runners take their jobs).
    // When the job finishes every session is recreated. One job runs at a time.
    public sealed class MultiRunnerCoordinator : RunnerService, IMultiRunnerCoordinator
    {
        private enum SlotState
        {
            Creating,       // session creation attempt in flight
            Listening,      // session active, long-poll armed
            Paused,         // session deleted while another slot runs a job
            Degraded,       // session creation failed; retried on a timer
            RestartPending, // config refreshed; rebuild before next session
            Dead,           // registration gone or credentials revoked
        }

        private sealed class Slot
        {
            public RegistrationRef Registration;
            public RunnerSettings Settings;
            public IConfigurationStore Store;
            public IRSAKeyManager KeyManager;
            public IRunnerServer RunnerServer;
            public IBrokerServer BrokerServer;
            public IMessageListener Listener;
            public IJobDispatcher Dispatcher;
            public SlotState State;
            public bool UsingMigratedSettings;
            public bool NeedsRebuild;
            public CancellationTokenSource PollCts;
            public Task<TaskAgentMessage> PollTask;
            public Task SessionTask;
            public DateTime NextSessionRetryUtc;

            public string DisplayName => Settings?.GitHubUrl ?? Registration.Slug;
        }

        private enum SessionAttemptOutcome
        {
            Success,
            Conflict,
            Failure,
            TimedOut,
            Error,
            Shutdown,
        }

        private static readonly TimeSpan SessionRetryDelay = TimeSpan.FromMinutes(5);
        // The listeners retry transient session-creation failures internally
        // (conflicts for ~4 minutes, connection errors indefinitely); bound each
        // attempt so one bad registration cannot stall the others.
        private static readonly TimeSpan SessionAttemptTimeout = TimeSpan.FromSeconds(90);
        private static readonly TimeSpan VersionCheckInterval = TimeSpan.FromHours(24);

        private readonly List<Slot> _slots = new();
        private ITerminal _term;
        private IErrorThrottler _acquireJobThrottler;
        private Slot _busySlot;
        private TaskCompletionSource<object> _jobIdleSignal;
        private TaskCompletionSource<object> _wakeSignal;
        private DateTime _nextVersionCheckUtc = DateTime.MinValue;

        // Test hooks: override construction of per-registration collaborators.
        public Func<RunnerSettings, IConfigurationStore, IRSAKeyManager, IRunnerServer, IBrokerServer, IMessageListener> ListenerFactory { get; set; }
        public Func<RunnerSettings, IRunnerServer, IConfigurationStore, IJobDispatcher> DispatcherFactory { get; set; }

        public override void Initialize(IHostContext hostContext)
        {
            base.Initialize(hostContext);
            _term = hostContext.GetService<ITerminal>();
            _acquireJobThrottler = hostContext.CreateService<IErrorThrottler>();
        }

        public async Task<int> RunAsync()
        {
            Trace.Info(nameof(RunAsync));
            var shutdownToken = HostContext.RunnerShutdownToken;

            // Validate directory permissions.
            string workDirectory = HostContext.GetDirectory(WellKnownDirectory.Work);
            Trace.Info($"Validating directory permissions for: '{workDirectory}'");
            try
            {
                Directory.CreateDirectory(workDirectory);
                IOUtil.ValidateExecutePermission(workDirectory);
            }
            catch (Exception ex)
            {
                Trace.Error(ex);
                _term.WriteError($"Fail to create and validate runner's work directory '{workDirectory}'.");
                return Constants.Runner.ReturnCode.TerminatedError;
            }

            var registrationStore = HostContext.GetService<IRegistrationStore>();
            foreach (var registration in registrationStore.GetAll().Where(x => !x.IsLegacy))
            {
                _slots.Add(BuildSlot(registration, preferMigratedSettings: true));
            }

            if (_slots.Count == 0)
            {
                _term.WriteError("No repository registrations found.");
                return Constants.Runner.ReturnCode.TerminatedError;
            }

            _term.WriteLine($"Current runner version: '{BuildConstants.RunnerPackage.Version}'");
            _term.WriteLine($"Configured repositories: {string.Join(", ", _slots.Select(x => x.DisplayName))}");

            var notification = HostContext.GetService<IJobNotification>();
            notification.StartClient(_slots[0].Settings.MonitorSocketAddress);

            // Create every registration's session in parallel.
            var startupResults = await Task.WhenAll(_slots.Select(x => CreateSessionForSlotAsync(x, shutdownToken)));
            if (!_slots.Any(x => x.State == SlotState.Listening))
            {
                if (startupResults.Any(x => x == CreateSessionResult.SessionConflict))
                {
                    return Constants.Runner.ReturnCode.SessionConflict;
                }

                return Constants.Runner.ReturnCode.TerminatedError;
            }

            try
            {
                return await RunLoopAsync(shutdownToken);
            }
            finally
            {
                await ShutdownSlotsAsync();
            }
        }

        private async Task<int> RunLoopAsync(CancellationToken shutdownToken)
        {
            while (!shutdownToken.IsCancellationRequested)
            {
                if (_slots.All(x => x.State == SlotState.Dead))
                {
                    _term.WriteError("All repository registrations have been removed or revoked. Shutting down.");
                    return Constants.Runner.ReturnCode.TerminatedError;
                }

                if (_busySlot == null)
                {
                    // Session (re)creation runs in the background so a slow or
                    // conflicted registration never blocks the others' polling.
                    foreach (var slot in _slots.Where(x => x.State == SlotState.RestartPending).ToList())
                    {
                        BeginRestartSlot(slot, shutdownToken);
                    }

                    foreach (var slot in _slots.Where(x => x.State == SlotState.Degraded && DateTime.UtcNow >= x.NextSessionRetryUtc).ToList())
                    {
                        BeginCreateSession(slot, shutdownToken);
                    }

                    // With automatic updates disabled, warn periodically when a
                    // newer runner version exists so this fork gets rebased
                    // before GitHub starts refusing the old version.
                    if (DateTime.UtcNow >= _nextVersionCheckUtc)
                    {
                        _nextVersionCheckUtc = DateTime.UtcNow + VersionCheckInterval;
                        _ = CheckRunnerVersionAsync(shutdownToken);
                    }
                }

                foreach (var slot in _slots.Where(x => x.State == SlotState.Listening && x.PollTask == null))
                {
                    StartPoll(slot);
                }

                if (_wakeSignal == null || _wakeSignal.Task.IsCompleted)
                {
                    _wakeSignal = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                }

                var waits = new List<Task>();
                waits.AddRange(_slots.Where(x => x.PollTask != null).Select(x => (Task)x.PollTask));
                waits.Add(_wakeSignal.Task);

                if (_busySlot != null && _jobIdleSignal != null)
                {
                    waits.Add(_jobIdleSignal.Task);
                }

                Task retryDelay = null;
                var nextRetryUtc = _slots
                    .Where(x => x.State == SlotState.Degraded)
                    .Select(x => x.NextSessionRetryUtc)
                    .DefaultIfEmpty(DateTime.MaxValue)
                    .Min();
                if (_busySlot == null && nextRetryUtc != DateTime.MaxValue)
                {
                    var delay = nextRetryUtc - DateTime.UtcNow;
                    if (delay < TimeSpan.FromSeconds(1))
                    {
                        delay = TimeSpan.FromSeconds(1);
                    }

                    retryDelay = HostContext.Delay(delay, shutdownToken);
                    waits.Add(retryDelay);
                }

                var completedTask = await Task.WhenAny(waits);
                if (shutdownToken.IsCancellationRequested)
                {
                    return Constants.Runner.ReturnCode.Success;
                }

                if (completedTask == retryDelay || completedTask == _wakeSignal.Task)
                {
                    continue;
                }

                if (_jobIdleSignal != null && completedTask == _jobIdleSignal.Task)
                {
                    ResumeSlots(shutdownToken);
                    continue;
                }

                var winner = _slots.FirstOrDefault(x => x.PollTask == completedTask);
                if (winner == null)
                {
                    continue;
                }

                winner.PollTask = null;
                TaskAgentMessage message;
                try
                {
                    message = await (Task<TaskAgentMessage>)completedTask;
                }
                catch (OperationCanceledException)
                {
                    continue;
                }
                catch (Exception ex) when (ex is TaskAgentNotFoundException || ex is RunnerNotFoundException)
                {
                    await HandleRegistrationGoneAsync(winner, ex);
                    continue;
                }
                catch (TaskAgentAccessTokenExpiredException)
                {
                    Trace.Info($"[{winner.DisplayName}] Runner OAuth token has been revoked.");
                    _term.WriteError($"The OAuth token for {winner.DisplayName} has been revoked. This registration stops listening.");
                    winner.State = SlotState.Dead;
                    continue;
                }
                catch (HostedRunnerDeprovisionedException)
                {
                    Trace.Info($"[{winner.DisplayName}] Hosted runner has been deprovisioned.");
                    winner.State = SlotState.Dead;
                    continue;
                }
                catch (AccessDeniedException ex) when (ex.ErrorCode == 1)
                {
                    // The service refuses runners below its minimum version.
                    Trace.Error(ex);
                    _term.WriteError($"GitHub rejected runner version {BuildConstants.RunnerPackage.Version} for {winner.DisplayName} as too old. Automatic updates are disabled in multi-repository mode - rebase and rebuild this fork to continue.");
                    await TeardownSessionQuietlyAsync(winner);
                    winner.State = SlotState.Dead;
                    continue;
                }
                catch (Exception ex)
                {
                    // The listener retries transient errors internally, so an
                    // exception here is close to fatal for the slot; drop the
                    // (possibly still active) session so the runner shows Offline
                    // and the retry doesn't conflict with it, then back off.
                    Trace.Error($"[{winner.DisplayName}] Message poll failed.");
                    Trace.Error(ex);
                    await TeardownSessionQuietlyAsync(winner);
                    MarkDegraded(winner);
                    continue;
                }

                if (message == null)
                {
                    continue;
                }

                var returnCode = await HandleMessageAsync(winner, message, shutdownToken);
                if (returnCode.HasValue)
                {
                    return returnCode.Value;
                }
            }

            return Constants.Runner.ReturnCode.Success;
        }

        private async Task<int?> HandleMessageAsync(Slot slot, TaskAgentMessage message, CancellationToken shutdownToken)
        {
            bool skipMessageDeletion = false;
            try
            {
                HostContext.WritePerfCounter($"MessageReceived_{message.MessageType}");
                if (string.Equals(message.MessageType, AgentRefreshMessage.MessageType, StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(message.MessageType, RunnerRefreshMessage.MessageType, StringComparison.OrdinalIgnoreCase))
                {
                    // Applying a stock runner update would replace this fork's
                    // binaries, breaking multi-repository support.
                    Trace.Info($"[{slot.DisplayName}] Received runner update request; automatic update is disabled in multi-repository mode.");
                    _term.WriteLine($"{DateTime.UtcNow:u}: GitHub requested a runner update. Automatic updates are disabled in multi-repository mode; rebase and rebuild this fork to update.", ConsoleColor.Yellow);
                }
                else if (string.Equals(message.MessageType, JobRequestMessageTypes.PipelineAgentJobRequest, StringComparison.OrdinalIgnoreCase))
                {
                    Trace.Info($"[{slot.DisplayName}] Received job message of length {message.Body.Length}, with hash '{IOUtil.GetSha256Hash(message.Body)}'");
                    var jobMessage = StringUtil.ConvertFromJson<Pipelines.AgentJobRequestMessage>(message.Body);
                    await AcquireAndRunJobAsync(slot, messageRef: null, jobMessage, shutdownToken);
                }
                else if (MessageUtil.IsRunServiceJob(message.MessageType))
                {
                    var messageRef = StringUtil.ConvertFromJson<RunnerJobRequestRef>(message.Body);
                    await AcquireAndRunJobAsync(slot, messageRef, jobMessage: null, shutdownToken);
                }
                else if (string.Equals(message.MessageType, JobCancelMessage.MessageType, StringComparison.OrdinalIgnoreCase))
                {
                    var cancelJobMessage = JsonUtility.FromString<JobCancelMessage>(message.Body);
                    slot.Dispatcher.Cancel(cancelJobMessage);
                }
                else if (string.Equals(message.MessageType, Pipelines.HostedRunnerShutdownMessage.MessageType, StringComparison.OrdinalIgnoreCase))
                {
                    var shutdownMessage = JsonUtility.FromString<Pipelines.HostedRunnerShutdownMessage>(message.Body);
                    Trace.Info($"[{slot.DisplayName}] Service requests the runner to shutdown. Reason: '{shutdownMessage.Reason}'.");
                    skipMessageDeletion = true;
                    return Constants.Runner.ReturnCode.Success;
                }
                else if (string.Equals(message.MessageType, TaskAgentMessageTypes.ForceTokenRefresh))
                {
                    Trace.Info($"[{slot.DisplayName}] Received ForceTokenRefreshMessage");
                    await slot.Listener.RefreshListenerTokenAsync();
                }
                else if (string.Equals(message.MessageType, RunnerRefreshConfigMessage.MessageType))
                {
                    var refreshConfigMessage = JsonUtility.FromString<RunnerRefreshConfigMessage>(message.Body);
                    Trace.Info($"[{slot.DisplayName}] Received RunnerRefreshConfigMessage for '{refreshConfigMessage.ConfigType}' config");
                    var configUpdater = new RunnerConfigUpdater(slot.Settings, slot.Store, slot.RunnerServer);
                    configUpdater.Initialize(HostContext);
                    await configUpdater.UpdateRunnerConfigAsync(
                        runnerQualifiedId: refreshConfigMessage.RunnerQualifiedId,
                        configType: refreshConfigMessage.ConfigType,
                        serviceType: refreshConfigMessage.ServiceType,
                        configRefreshUrl: refreshConfigMessage.ConfigRefreshUrl);

                    if (string.Equals(refreshConfigMessage.ConfigType, "runner", StringComparison.OrdinalIgnoreCase))
                    {
                        Trace.Info($"[{slot.DisplayName}] Runner configuration was updated; scheduling session restart.");
                        slot.State = SlotState.RestartPending;
                    }
                }
                else
                {
                    Trace.Error($"[{slot.DisplayName}] Received message {message.MessageId} with unsupported message type {message.MessageType}.");
                }
            }
            finally
            {
                if (!skipMessageDeletion)
                {
                    try
                    {
                        await slot.Listener.DeleteMessageAsync(message);
                    }
                    catch (Exception ex)
                    {
                        Trace.Error($"[{slot.DisplayName}] Catch exception during delete message from message queue. message id: {message.MessageId}");
                        Trace.Error(ex);
                    }
                }
            }

            return null;
        }

        private async Task AcquireAndRunJobAsync(Slot winner, RunnerJobRequestRef messageRef, Pipelines.AgentJobRequestMessage jobMessage, CancellationToken shutdownToken)
        {
            // Take the other registrations offline first so GitHub can route
            // their queued jobs to other available runners.
            await SuspendOtherSlotsAsync(winner);

            if (messageRef != null)
            {
                // Broker flow: the poll only delivered a reference; acknowledge
                // and fetch the actual job message now.
                if (messageRef.ShouldAcknowledge)
                {
                    try
                    {
                        await winner.Listener.AcknowledgeMessageAsync(messageRef.RunnerRequestId, shutdownToken);
                    }
                    catch (Exception ex)
                    {
                        Trace.Error($"[{winner.DisplayName}] Best-effort acknowledge failed for request '{messageRef.RunnerRequestId}'");
                        Trace.Error(ex);
                    }
                }

                var credMgr = HostContext.GetService<ICredentialManager>();
                try
                {
                    if (string.IsNullOrEmpty(messageRef.RunServiceUrl))
                    {
                        var creds = credMgr.LoadCredentials(allowAuthUrlV2: false, winner.Store, winner.KeyManager);
                        var actionsRunServer = HostContext.CreateService<IActionsRunServer>();
                        await actionsRunServer.ConnectAsync(new Uri(winner.Settings.ServerUrl), creds);
                        jobMessage = await actionsRunServer.GetJobMessageAsync(messageRef.RunnerRequestId, shutdownToken);
                    }
                    else
                    {
                        var credsV2 = credMgr.LoadCredentials(allowAuthUrlV2: true, winner.Store, winner.KeyManager);
                        var runServer = HostContext.CreateService<IRunServer>();
                        await runServer.ConnectAsync(new Uri(messageRef.RunServiceUrl), credsV2);
                        try
                        {
                            jobMessage = await runServer.GetJobMessageAsync(messageRef.RunnerRequestId, messageRef.BillingOwnerId, shutdownToken);
                            _acquireJobThrottler.Reset();
                        }
                        catch (Exception ex) when (
                            ex is TaskOrchestrationJobNotFoundException ||          // HTTP status 404
                            ex is TaskOrchestrationJobAlreadyAcquiredException ||   // HTTP status 409
                            ex is TaskOrchestrationJobUnprocessableException)       // HTTP status 422
                        {
                            Trace.Info($"[{winner.DisplayName}] Skipping message Job. {ex.Message}");
                            await _acquireJobThrottler.IncrementAndWaitAsync(shutdownToken);
                            jobMessage = null;
                        }
                        catch (Exception ex)
                        {
                            Trace.Error($"[{winner.DisplayName}] Caught exception from acquiring job message: {ex}");
                            if (HostContext.AllowAuthMigration)
                            {
                                Trace.Info("Disable migration mode for 60 minutes.");
                                HostContext.DeferAuthMigration(TimeSpan.FromMinutes(60), $"Acquire job failed with exception: {ex}");
                            }

                            jobMessage = null;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Trace.Error($"[{winner.DisplayName}] Failed to acquire job message.");
                    Trace.Error(ex);
                    jobMessage = null;
                }
            }

            if (jobMessage == null)
            {
                // The job evaporated (finished, cancelled, or taken elsewhere);
                // bring everyone back online.
                ResumeSlots(shutdownToken);
                return;
            }

            _busySlot = winner;
            _jobIdleSignal = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            _term.WriteLine($"{DateTime.UtcNow:u}: Running job for {winner.DisplayName}");
            winner.Dispatcher.Run(jobMessage, runOnce: false);
        }

        private async Task SuspendOtherSlotsAsync(Slot winner)
        {
            var toSuspend = _slots.Where(x => x != winner && x.State == SlotState.Listening).ToList();
            if (toSuspend.Count == 0)
            {
                return;
            }

            // Cancelling the poll token unblocks the long poll immediately.
            foreach (var slot in toSuspend)
            {
                slot.PollCts?.Cancel();
            }

            foreach (var slot in toSuspend)
            {
                if (slot.PollTask != null)
                {
                    try
                    {
                        var racedMessage = await slot.PollTask;
                        if (racedMessage != null)
                        {
                            // Neither acknowledged nor deleted: the server holds
                            // the job and redelivers it once a session is back.
                            // The listener recorded this message id as its cursor,
                            // though, so rebuild the listener on resume — a stale
                            // cursor could otherwise skip the redelivery.
                            Trace.Info($"[{slot.DisplayName}] Leaving raced message {racedMessage.MessageId} ({racedMessage.MessageType}) untouched; the listener will be rebuilt so the server redelivers it.");
                            slot.NeedsRebuild = true;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (Exception ex)
                    {
                        Trace.Error($"[{slot.DisplayName}] Poll ended with error while suspending.");
                        Trace.Error(ex);
                    }

                    slot.PollTask = null;
                }
            }

            await Task.WhenAll(toSuspend.Select(async slot =>
            {
                try
                {
                    await slot.Listener.DeleteSessionAsync();
                }
                catch (Exception ex)
                {
                    // A leaked session ages out server-side.
                    Trace.Error($"[{slot.DisplayName}] Failed to delete session while suspending.");
                    Trace.Error(ex);
                }

                slot.State = SlotState.Paused;
            }));

            _term.WriteLine($"{DateTime.UtcNow:u}: Paused listening for {string.Join(", ", toSuspend.Select(x => x.DisplayName))} while a job runs");
        }

        // Brings suspended registrations back online. Session creation runs in the
        // background per slot; each prints "[...] Listening for Jobs" as it lands.
        private void ResumeSlots(CancellationToken shutdownToken)
        {
            _busySlot = null;
            _jobIdleSignal = null;

            foreach (var slot in _slots.Where(x => x.State == SlotState.RestartPending).ToList())
            {
                BeginRestartSlot(slot, shutdownToken);
            }

            foreach (var slot in _slots.Where(x => x.State == SlotState.Paused).ToList())
            {
                if (slot.NeedsRebuild)
                {
                    RebuildSlot(slot, preferMigratedSettings: true);
                }

                BeginCreateSession(slot, shutdownToken);
            }
        }

        private async Task<CreateSessionResult> CreateSessionForSlotAsync(Slot slot, CancellationToken shutdownToken)
        {
            slot.State = SlotState.Creating;
            var outcome = await TryCreateSessionOnceAsync(slot, shutdownToken);

            // Mirror the single-config runner: on ANY non-success result with
            // migrated settings (failure, conflict, timeout), fall back to the
            // original .runner settings and try once more.
            if (outcome != SessionAttemptOutcome.Success &&
                outcome != SessionAttemptOutcome.Shutdown &&
                slot.UsingMigratedSettings)
            {
                Trace.Warning($"[{slot.DisplayName}] Session creation with migrated settings did not succeed ({outcome}); falling back to original settings.");
                RebuildSlot(slot, preferMigratedSettings: false);
                slot.State = SlotState.Creating;
                outcome = await TryCreateSessionOnceAsync(slot, shutdownToken);
            }

            switch (outcome)
            {
                case SessionAttemptOutcome.Success:
                    if (_busySlot != null && !ReferenceEquals(slot, _busySlot))
                    {
                        // A job started while this session was being created; go
                        // straight back offline until it finishes.
                        Trace.Info($"[{slot.DisplayName}] Session created while a job is running; pausing again.");
                        await TeardownSessionQuietlyAsync(slot);
                        slot.State = SlotState.Paused;
                        return CreateSessionResult.Success;
                    }

                    slot.State = SlotState.Listening;
                    _term.WriteLine($"{DateTime.UtcNow:u}: [{slot.DisplayName}] Listening for Jobs");
                    return CreateSessionResult.Success;

                case SessionAttemptOutcome.Conflict:
                    // Another process holds this registration's session; keep
                    // the other registrations alive and retry later.
                    _term.WriteError($"A session for {slot.DisplayName} already exists elsewhere. Retrying in {SessionRetryDelay.TotalMinutes:0} minutes.");
                    MarkDegraded(slot);
                    return CreateSessionResult.SessionConflict;

                case SessionAttemptOutcome.Shutdown:
                    slot.State = SlotState.Paused;
                    return CreateSessionResult.Failure;

                case SessionAttemptOutcome.Failure:
                    _term.WriteError($"Failed to create a session for {slot.DisplayName}. This registration stops listening.");
                    slot.State = SlotState.Dead;
                    return CreateSessionResult.Failure;

                default:
                    // TimedOut / Error: worth retrying on the degraded timer.
                    MarkDegraded(slot);
                    return CreateSessionResult.Failure;
            }
        }

        // One bounded session-creation attempt. Never throws.
        private async Task<SessionAttemptOutcome> TryCreateSessionOnceAsync(Slot slot, CancellationToken shutdownToken)
        {
            try
            {
                using (var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken))
                {
                    attemptCts.CancelAfter(SessionAttemptTimeout);
                    var result = await slot.Listener.CreateSessionAsync(attemptCts.Token);
                    switch (result)
                    {
                        case CreateSessionResult.Success:
                            return SessionAttemptOutcome.Success;
                        case CreateSessionResult.SessionConflict:
                            return SessionAttemptOutcome.Conflict;
                        default:
                            return SessionAttemptOutcome.Failure;
                    }
                }
            }
            catch (OperationCanceledException) when (shutdownToken.IsCancellationRequested)
            {
                return SessionAttemptOutcome.Shutdown;
            }
            catch (OperationCanceledException)
            {
                Trace.Warning($"[{slot.DisplayName}] Session creation attempt timed out after {SessionAttemptTimeout.TotalSeconds:0}s.");
                return SessionAttemptOutcome.TimedOut;
            }
            catch (Exception ex)
            {
                Trace.Error($"[{slot.DisplayName}] Session creation threw.");
                Trace.Error(ex);
                return SessionAttemptOutcome.Error;
            }
        }

        // Launches a session-creation attempt in the background; completion pulses
        // the run loop awake so it can arm the new poll.
        private void BeginCreateSession(Slot slot, CancellationToken shutdownToken)
        {
            if (slot.State == SlotState.Creating && slot.SessionTask?.IsCompleted == false)
            {
                return;
            }

            slot.State = SlotState.Creating;
            slot.SessionTask = Task.Run(async () =>
            {
                try
                {
                    await CreateSessionForSlotAsync(slot, shutdownToken);
                }
                finally
                {
                    PulseWake();
                }
            });
        }

        private void PulseWake()
        {
            _wakeSignal?.TrySetResult(null);
        }

        private async Task TeardownSessionQuietlyAsync(Slot slot)
        {
            try
            {
                await slot.Listener.DeleteSessionAsync();
            }
            catch (Exception ex)
            {
                Trace.Error($"[{slot.DisplayName}] Failed to delete session.");
                Trace.Error(ex);
            }
        }

        private void MarkDegraded(Slot slot)
        {
            slot.State = SlotState.Degraded;
            slot.NextSessionRetryUtc = DateTime.UtcNow + SessionRetryDelay;
        }

        private void StartPoll(Slot slot)
        {
            slot.PollCts?.Dispose();
            slot.PollCts = CancellationTokenSource.CreateLinkedTokenSource(HostContext.RunnerShutdownToken);
            slot.PollTask = slot.Listener.GetNextMessageAsync(slot.PollCts.Token);
        }

        // Rebuilds a slot after a config refresh: drain its poll, drop its session,
        // reload settings, and create a fresh session — all in the background so
        // the other registrations keep polling meanwhile.
        private void BeginRestartSlot(Slot slot, CancellationToken shutdownToken)
        {
            if (slot.State == SlotState.Creating && slot.SessionTask?.IsCompleted == false)
            {
                return;
            }

            Trace.Info($"[{slot.DisplayName}] Restarting listener after config refresh.");
            slot.PollCts?.Cancel();
            var pollTask = slot.PollTask;
            slot.PollTask = null;
            var listener = slot.Listener;
            slot.State = SlotState.Creating;
            slot.SessionTask = Task.Run(async () =>
            {
                try
                {
                    if (pollTask != null)
                    {
                        try
                        {
                            await pollTask;
                        }
                        catch (Exception)
                        {
                        }
                    }

                    try
                    {
                        await listener.DeleteSessionAsync();
                    }
                    catch (Exception ex)
                    {
                        Trace.Error($"[{slot.DisplayName}] Failed to delete session before restart.");
                        Trace.Error(ex);
                    }

                    RebuildSlot(slot, preferMigratedSettings: true);
                    await CreateSessionForSlotAsync(slot, shutdownToken);
                }
                finally
                {
                    PulseWake();
                }
            });
        }

        private async Task HandleRegistrationGoneAsync(Slot slot, Exception ex)
        {
            Trace.Info($"[{slot.DisplayName}] Runner registration no longer exists on the server. {ex.Message}");
            _term.WriteError($"The runner for {slot.DisplayName} no longer exists on the server. Removing its local configuration.");
            slot.State = SlotState.Dead;
            slot.PollCts?.Cancel();
            slot.PollTask = null;

            try
            {
                var registrationStore = HostContext.GetService<IRegistrationStore>();
                registrationStore.DeleteRegistration(slot.Registration.Slug);
            }
            catch (Exception deleteEx)
            {
                Trace.Error($"[{slot.DisplayName}] Failed to delete local registration files.");
                Trace.Error(deleteEx);
            }

            await Task.CompletedTask;
        }

        private async Task ShutdownSlotsAsync()
        {
            Trace.Info("Shutting down all registration listeners.");
            foreach (var slot in _slots)
            {
                slot.PollCts?.Cancel();
            }

            // Let in-flight background session creations finish (they observe the
            // shutdown token) before deleting sessions underneath them.
            foreach (var slot in _slots)
            {
                if (slot.SessionTask != null)
                {
                    try
                    {
                        await slot.SessionTask;
                    }
                    catch (Exception)
                    {
                    }

                    slot.SessionTask = null;
                }
            }

            foreach (var slot in _slots)
            {
                if (slot.PollTask != null)
                {
                    try
                    {
                        await slot.PollTask;
                    }
                    catch (Exception)
                    {
                    }

                    slot.PollTask = null;
                }
            }

            foreach (var slot in _slots)
            {
                try
                {
                    await slot.Dispatcher.ShutdownAsync();
                }
                catch (Exception ex)
                {
                    Trace.Error($"[{slot.DisplayName}] Dispatcher shutdown failed.");
                    Trace.Error(ex);
                }
            }

            await Task.WhenAll(_slots.Where(x => x.State != SlotState.Dead).Select(async slot =>
            {
                try
                {
                    Trace.Info($"[{slot.DisplayName}] Deleting session...");
                    await slot.Listener.DeleteSessionAsync();
                }
                catch (Exception ex)
                {
                    Trace.Error($"[{slot.DisplayName}] Failed to delete session during shutdown.");
                    Trace.Error(ex);
                }
            }));
        }

        private Slot BuildSlot(RegistrationRef registration, bool preferMigratedSettings)
        {
            var slot = new Slot { Registration = registration };
            PopulateSlot(slot, preferMigratedSettings);
            return slot;
        }

        // (Re)creates a slot's per-registration collaborators from its on-disk
        // configuration.
        private void PopulateSlot(Slot slot, bool preferMigratedSettings)
        {
            var registration = slot.Registration;
            var store = HostContext.CreateService<IConfigurationStore>();
            store.ConfigDirectoryOverride = registration.Directory;

            var keyManager = HostContext.CreateService<IRSAKeyManager>();
            keyManager.KeyFileOverride = Path.Combine(registration.Directory, ".credentials_rsaparams");

            RunnerSettings settings = null;
            bool usingMigrated = false;
            if (preferMigratedSettings && store.IsMigratedConfigured())
            {
                try
                {
                    settings = store.GetMigratedSettings();
                    usingMigrated = true;
                }
                catch (Exception ex)
                {
                    Trace.Warning($"[{registration.Slug}] Failed to load migrated settings: {ex.Message}");
                }
            }

            settings ??= store.GetSettings();

            var runnerServer = HostContext.CreateService<IRunnerServer>();
            var brokerServer = HostContext.CreateService<IBrokerServer>();

            IMessageListener listener;
            if (ListenerFactory != null)
            {
                listener = ListenerFactory(settings, store, keyManager, runnerServer, brokerServer);
            }
            else if (settings.UseV2Flow)
            {
                var brokerListener = new BrokerMessageListener(settings, store, keyManager, runnerServer, brokerServer, usingMigrated);
                brokerListener.Initialize(HostContext);
                listener = brokerListener;
            }
            else
            {
                var messageListener = new MessageListener(settings, store, keyManager, runnerServer, brokerServer);
                messageListener.Initialize(HostContext);
                listener = messageListener;
            }

            IJobDispatcher dispatcher;
            if (DispatcherFactory != null)
            {
                dispatcher = DispatcherFactory(settings, runnerServer, store);
            }
            else
            {
                var jobDispatcher = new JobDispatcher(settings, runnerServer, store);
                jobDispatcher.Initialize(HostContext);
                jobDispatcher.WorkerEnvironment = new Dictionary<string, string>
                {
                    [Constants.MultiConfig.ActiveConfigEnvVar] = registration.Slug,
                };
                dispatcher = jobDispatcher;
            }

            // The listener re-polls with Busy/Online when its jobs start/finish;
            // the coordinator resumes the other registrations on the Online edge.
            dispatcher.JobStatus += listener.OnJobStatus;
            dispatcher.JobStatus += (sender, e) => OnSlotJobStatus(slot, e);

            slot.Settings = settings;
            slot.Store = store;
            slot.KeyManager = keyManager;
            slot.RunnerServer = runnerServer;
            slot.BrokerServer = brokerServer;
            slot.Listener = listener;
            slot.Dispatcher = dispatcher;
            slot.State = SlotState.Paused;
            slot.UsingMigratedSettings = usingMigrated;
            slot.NeedsRebuild = false;
        }

        private void RebuildSlot(Slot slot, bool preferMigratedSettings)
        {
            PopulateSlot(slot, preferMigratedSettings);
        }

        private void OnSlotJobStatus(Slot slot, JobStatusEventArgs e)
        {
            if (e.Status == TaskAgentStatus.Online && ReferenceEquals(slot, _busySlot))
            {
                _jobIdleSignal?.TrySetResult(null);
            }
        }

        // Best-effort: compare the running version against the newest package the
        // service offers and warn when behind. Never fails the runner.
        private async Task CheckRunnerVersionAsync(CancellationToken token)
        {
            try
            {
                foreach (var slot in _slots.Where(x => x.State == SlotState.Listening).ToList())
                {
                    List<PackageMetadata> packages;
                    try
                    {
                        packages = await slot.RunnerServer.GetPackagesAsync("agent", BuildConstants.RunnerPackage.PackageName, 1, false, token);
                    }
                    catch (Exception ex)
                    {
                        // Broker-only registrations may not serve the package API.
                        Trace.Info($"[{slot.DisplayName}] Package version check not available: {ex.Message}");
                        continue;
                    }

                    var latest = packages?.FirstOrDefault();
                    if (latest == null)
                    {
                        continue;
                    }

                    var serverVersion = new PackageVersion(latest.Version);
                    var currentVersion = new PackageVersion(BuildConstants.RunnerPackage.Version);
                    if (serverVersion.CompareTo(currentVersion) > 0)
                    {
                        _term.WriteLine($"{DateTime.UtcNow:u}: Warning: this runner is version {BuildConstants.RunnerPackage.Version} but the latest runner version is {latest.Version}. Automatic updates are disabled in multi-repository mode - rebase and rebuild this fork, or GitHub may eventually refuse this version.", ConsoleColor.Yellow);
                    }
                    else
                    {
                        Trace.Info($"Runner version {BuildConstants.RunnerPackage.Version} is current (latest available: {latest.Version}).");
                    }

                    return;
                }
            }
            catch (Exception)
            {
                // Version awareness must never take the runner down.
            }
        }
    }
}
