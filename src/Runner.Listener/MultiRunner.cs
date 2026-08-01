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
            public CancellationTokenSource PollCts;
            public Task<TaskAgentMessage> PollTask;
            public DateTime NextSessionRetryUtc;

            public string DisplayName => Settings?.GitHubUrl ?? Registration.Slug;
        }

        private static readonly TimeSpan SessionRetryDelay = TimeSpan.FromMinutes(5);

        private readonly List<Slot> _slots = new();
        private ITerminal _term;
        private IErrorThrottler _acquireJobThrottler;
        private Slot _busySlot;
        private TaskCompletionSource<object> _jobIdleSignal;

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
                    foreach (var slot in _slots.Where(x => x.State == SlotState.RestartPending).ToList())
                    {
                        await RestartSlotAsync(slot, shutdownToken);
                    }

                    foreach (var slot in _slots.Where(x => x.State == SlotState.Degraded && DateTime.UtcNow >= x.NextSessionRetryUtc).ToList())
                    {
                        await CreateSessionForSlotAsync(slot, shutdownToken);
                    }
                }

                foreach (var slot in _slots.Where(x => x.State == SlotState.Listening && x.PollTask == null))
                {
                    StartPoll(slot);
                }

                var waits = new List<Task>();
                waits.AddRange(_slots.Where(x => x.PollTask != null).Select(x => (Task)x.PollTask));

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

                if (waits.Count == 0)
                {
                    _term.WriteError("No repository registrations are listening and none can recover. Shutting down.");
                    return Constants.Runner.ReturnCode.TerminatedError;
                }

                var completedTask = await Task.WhenAny(waits);
                if (shutdownToken.IsCancellationRequested)
                {
                    return Constants.Runner.ReturnCode.Success;
                }

                if (completedTask == retryDelay)
                {
                    continue;
                }

                if (_jobIdleSignal != null && completedTask == _jobIdleSignal.Task)
                {
                    await ResumeSlotsAsync(shutdownToken);
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
                catch (Exception ex)
                {
                    // The listener retries transient errors internally, so an
                    // exception here is close to fatal for the slot; back off and
                    // rebuild the session later.
                    Trace.Error($"[{winner.DisplayName}] Message poll failed.");
                    Trace.Error(ex);
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
                await ResumeSlotsAsync(shutdownToken);
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
                            Trace.Info($"[{slot.DisplayName}] Leaving raced message {racedMessage.MessageId} ({racedMessage.MessageType}) untouched; the server will redeliver it.");
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

        private async Task ResumeSlotsAsync(CancellationToken shutdownToken)
        {
            _busySlot = null;
            _jobIdleSignal = null;

            foreach (var slot in _slots.Where(x => x.State == SlotState.RestartPending).ToList())
            {
                await RestartSlotAsync(slot, shutdownToken);
            }

            var toResume = _slots.Where(x => x.State == SlotState.Paused).ToList();
            if (toResume.Count > 0)
            {
                await Task.WhenAll(toResume.Select(x => CreateSessionForSlotAsync(x, shutdownToken)));
                var resumed = toResume.Where(x => x.State == SlotState.Listening).Select(x => x.DisplayName).ToList();
                if (resumed.Count > 0)
                {
                    _term.WriteLine($"{DateTime.UtcNow:u}: Resumed listening for {string.Join(", ", resumed)}");
                }
            }
        }

        private async Task<CreateSessionResult> CreateSessionForSlotAsync(Slot slot, CancellationToken shutdownToken)
        {
            try
            {
                var result = await slot.Listener.CreateSessionAsync(shutdownToken);
                switch (result)
                {
                    case CreateSessionResult.Success:
                        slot.State = SlotState.Listening;
                        _term.WriteLine($"{DateTime.UtcNow:u}: [{slot.DisplayName}] Listening for Jobs");
                        return result;

                    case CreateSessionResult.SessionConflict:
                        // Another process holds this registration's session; keep
                        // the other registrations alive and retry later.
                        _term.WriteError($"A session for {slot.DisplayName} already exists elsewhere. Retrying in {SessionRetryDelay.TotalMinutes:0} minutes.");
                        MarkDegraded(slot);
                        return result;

                    default:
                        if (slot.UsingMigratedSettings)
                        {
                            // Mirror the single-config fallback from migrated to
                            // original settings.
                            Trace.Warning($"[{slot.DisplayName}] Session creation failed with migrated settings; falling back to original settings.");
                            RebuildSlot(slot, preferMigratedSettings: false);
                            return await CreateSessionForSlotAsync(slot, shutdownToken);
                        }

                        _term.WriteError($"Failed to create a session for {slot.DisplayName}. This registration stops listening.");
                        slot.State = SlotState.Dead;
                        return result;
                }
            }
            catch (OperationCanceledException) when (shutdownToken.IsCancellationRequested)
            {
                slot.State = SlotState.Paused;
                return CreateSessionResult.Failure;
            }
            catch (Exception ex)
            {
                Trace.Error($"[{slot.DisplayName}] Session creation threw.");
                Trace.Error(ex);
                MarkDegraded(slot);
                return CreateSessionResult.Failure;
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

        private async Task RestartSlotAsync(Slot slot, CancellationToken shutdownToken)
        {
            Trace.Info($"[{slot.DisplayName}] Restarting listener after config refresh.");
            slot.PollCts?.Cancel();
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

            try
            {
                await slot.Listener.DeleteSessionAsync();
            }
            catch (Exception ex)
            {
                Trace.Error($"[{slot.DisplayName}] Failed to delete session before restart.");
                Trace.Error(ex);
            }

            RebuildSlot(slot, preferMigratedSettings: true);
            await CreateSessionForSlotAsync(slot, shutdownToken);
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
                var brokerListener = new BrokerMessageListener(settings, store, keyManager, runnerServer, brokerServer);
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
    }
}
