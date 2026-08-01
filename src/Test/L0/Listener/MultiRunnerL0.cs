using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using GitHub.DistributedTask.WebApi;
using GitHub.Runner.Listener;
using GitHub.Runner.Listener.Configuration;
using GitHub.Runner.Sdk;
using GitHub.Services.WebApi;
using Moq;
using Xunit;
using Pipelines = GitHub.DistributedTask.Pipelines;

namespace GitHub.Runner.Common.Tests.Listener
{
    public sealed class MultiRunnerL0 : IDisposable
    {
        private readonly string _tempRoot;
        private readonly RegistrationStore _registrationStore;
        private readonly Mock<IJobNotification> _jobNotification = new();
        private readonly Mock<IErrorThrottler> _acquireJobThrottler = new();

        public MultiRunnerL0()
        {
            _tempRoot = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("D"));
            Directory.CreateDirectory(_tempRoot);
            _registrationStore = new RegistrationStore
            {
                RootDirectory = _tempRoot,
            };
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(_tempRoot, recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        private sealed class TestSlot
        {
            public Mock<IMessageListener> Listener = new();
            public Mock<IJobDispatcher> Dispatcher = new();
            public int CreateSessionCalls;
            public int PollCalls;
        }

        private void SeedRegistration(string slug, string gitHubUrl, bool alsoMigrated = false)
        {
            var dir = Path.Combine(_tempRoot, Constants.MultiConfig.RegistrationsDirectory, slug);
            Directory.CreateDirectory(dir);
            var settings = new RunnerSettings
            {
                AgentId = (ulong)(slug.GetHashCode() & 0x7FFFFFFF),
                AgentName = "test-runner",
                GitHubUrl = gitHubUrl,
                ServerUrl = "http://localhost/tenant",
                WorkFolder = "_work",
            };
            IOUtil.SaveObject(settings, Path.Combine(dir, ".runner"));
            if (alsoMigrated)
            {
                IOUtil.SaveObject(settings, Path.Combine(dir, ".runner_migrated"));
            }
        }

        private TestHostContext CreateTestContext(Dictionary<string, TestSlot> slots, [System.Runtime.CompilerServices.CallerMemberName] string testName = "")
        {
            var hc = new TestHostContext(this, testName);
            hc.SetSingleton<IRegistrationStore>(_registrationStore);
            hc.SetSingleton<IJobNotification>(_jobNotification.Object);
            hc.EnqueueInstance<IErrorThrottler>(_acquireJobThrottler.Object);

            foreach (var _ in slots)
            {
                // Multiple sets per slot: slot rebuilds (raced messages, migrated
                // fallback) construct a fresh collaborator set each time.
                for (int i = 0; i < 3; i++)
                {
                    hc.EnqueueInstance<IConfigurationStore>(new ConfigurationStore());
                    var keyManager = new Mock<IRSAKeyManager>();
                    keyManager.SetupAllProperties();
                    hc.EnqueueInstance<IRSAKeyManager>(keyManager.Object);
                    hc.EnqueueInstance<IRunnerServer>(new Mock<IRunnerServer>().Object);
                    hc.EnqueueInstance<IBrokerServer>(new Mock<IBrokerServer>().Object);
                }
            }

            return hc;
        }

        private MultiRunnerCoordinator CreateCoordinator(TestHostContext hc, Dictionary<string, TestSlot> slots, Dictionary<string, int> listenerFactoryCalls = null)
        {
            var coordinator = new MultiRunnerCoordinator
            {
                // Route each registration to its scripted listener/dispatcher by
                // the GitHub URL in its settings.
                ListenerFactory = (settings, store, keyManager, runnerServer, brokerServer) =>
                {
                    if (listenerFactoryCalls != null)
                    {
                        lock (listenerFactoryCalls)
                        {
                            listenerFactoryCalls[settings.GitHubUrl] = listenerFactoryCalls.GetValueOrDefault(settings.GitHubUrl) + 1;
                        }
                    }

                    return slots[settings.GitHubUrl].Listener.Object;
                },
                DispatcherFactory = (settings, runnerServer, store) => slots[settings.GitHubUrl].Dispatcher.Object,
            };
            coordinator.Initialize(hc);
            return coordinator;
        }

        private static TaskAgentMessage CreateJobMessage()
        {
            TaskOrchestrationPlanReference plan = new();
            TimelineReference timeline = null;
            Guid jobId = Guid.NewGuid();
            var jobMessage = new Pipelines.AgentJobRequestMessage(plan, timeline, jobId, "test", "test", null, null, null, new Dictionary<string, VariableValue>(), new List<MaskHint>(), new Pipelines.JobResources(), new Pipelines.ContextData.DictionaryContextData(), new Pipelines.WorkspaceOptions(), new List<Pipelines.ActionStep>(), null, null, null, null, null);
            return new TaskAgentMessage
            {
                MessageId = 4234,
                MessageType = JobRequestMessageTypes.PipelineAgentJobRequest,
                Body = JsonUtility.ToString(jobMessage),
            };
        }

        private static void SetupSessionSuccess(TestSlot slot)
        {
            slot.Listener
                .Setup(x => x.CreateSessionAsync(It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    slot.CreateSessionCalls++;
                    return Task.FromResult(CreateSessionResult.Success);
                });
        }

        private static void SetupBlockingPoll(TestSlot slot)
        {
            slot.Listener
                .Setup(x => x.GetNextMessageAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(async token =>
                {
                    slot.PollCalls++;
                    var tcs = new TaskCompletionSource<TaskAgentMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
                    using (token.Register(() => tcs.TrySetCanceled()))
                    {
                        return await tcs.Task;
                    }
                });
        }

        private static async Task WaitForAsync(Func<bool> condition, string description)
        {
            for (int i = 0; i < 1000; i++)
            {
                if (condition())
                {
                    return;
                }

                await Task.Delay(10);
            }

            throw new TimeoutException($"Timed out waiting for: {description}");
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Runner")]
        public async Task JobOnOneRepositorySuspendsOthersAndResumesAfter()
        {
            var slotA = new TestSlot();
            var slotB = new TestSlot();
            var slots = new Dictionary<string, TestSlot>
            {
                ["https://github.com/apfritts/repo-a"] = slotA,
                ["https://github.com/apfritts/repo-b"] = slotB,
            };
            SeedRegistration("repo-a", "https://github.com/apfritts/repo-a");
            SeedRegistration("repo-b", "https://github.com/apfritts/repo-b");

            using (var hc = CreateTestContext(slots))
            {
                SetupSessionSuccess(slotA);
                SetupSessionSuccess(slotB);
                SetupBlockingPoll(slotB);

                // Repo A delivers one job, then blocks.
                var jobMessage = CreateJobMessage();
                slotA.Listener
                    .Setup(x => x.GetNextMessageAsync(It.IsAny<CancellationToken>()))
                    .Returns<CancellationToken>(async token =>
                    {
                        if (Interlocked.Increment(ref slotA.PollCalls) == 1)
                        {
                            return jobMessage;
                        }

                        var tcs = new TaskCompletionSource<TaskAgentMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
                        using (token.Register(() => tcs.TrySetCanceled()))
                        {
                            return await tcs.Task;
                        }
                    });

                bool jobDispatched = false;
                slotA.Dispatcher
                    .Setup(x => x.Run(It.IsAny<Pipelines.AgentJobRequestMessage>(), false))
                    .Callback(() => jobDispatched = true);

                var coordinator = CreateCoordinator(hc, slots);
                var runTask = coordinator.RunAsync();

                // The job from repo A takes repo B offline before dispatching.
                await WaitForAsync(() => jobDispatched, "job dispatched");
                slotB.Listener.Verify(x => x.DeleteSessionAsync(), Times.Once);
                Assert.Equal(1, slotB.CreateSessionCalls);

                // Signal job completion; repo B's session is recreated.
                slotA.Dispatcher.Raise(x => x.JobStatus += null, new JobStatusEventArgs(TaskAgentStatus.Online));
                await WaitForAsync(() => slotB.CreateSessionCalls >= 2, "repo B session recreated");

                // The processed job message is deleted from repo A's queue.
                slotA.Listener.Verify(x => x.DeleteMessageAsync(jobMessage), Times.Once);

                hc.ShutdownRunner(ShutdownReason.UserCancelled);
                Assert.Equal(Constants.Runner.ReturnCode.Success, await runTask);
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Runner")]
        public async Task RacedMessageOnLoserIsNeitherAcknowledgedNorDeleted()
        {
            var slotA = new TestSlot();
            var slotB = new TestSlot();
            var slots = new Dictionary<string, TestSlot>
            {
                ["https://github.com/apfritts/repo-a"] = slotA,
                ["https://github.com/apfritts/repo-b"] = slotB,
            };
            SeedRegistration("repo-a", "https://github.com/apfritts/repo-a");
            SeedRegistration("repo-b", "https://github.com/apfritts/repo-b");

            using (var hc = CreateTestContext(slots))
            {
                SetupSessionSuccess(slotA);
                SetupSessionSuccess(slotB);

                var jobMessageA = CreateJobMessage();
                slotA.Listener
                    .Setup(x => x.GetNextMessageAsync(It.IsAny<CancellationToken>()))
                    .Returns<CancellationToken>(async token =>
                    {
                        if (Interlocked.Increment(ref slotA.PollCalls) == 1)
                        {
                            return jobMessageA;
                        }

                        var tcs = new TaskCompletionSource<TaskAgentMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
                        using (token.Register(() => tcs.TrySetCanceled()))
                        {
                            return await tcs.Task;
                        }
                    });

                // Repo B's poll races: it completes with a job message exactly
                // when the coordinator cancels the poll to suspend it.
                var racedMessage = CreateJobMessage();
                slotB.Listener
                    .Setup(x => x.GetNextMessageAsync(It.IsAny<CancellationToken>()))
                    .Returns<CancellationToken>(async token =>
                    {
                        if (Interlocked.Increment(ref slotB.PollCalls) == 1)
                        {
                            var tcs = new TaskCompletionSource<TaskAgentMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
                            using (token.Register(() => tcs.TrySetResult(racedMessage)))
                            {
                                return await tcs.Task;
                            }
                        }

                        var blocked = new TaskCompletionSource<TaskAgentMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
                        using (token.Register(() => blocked.TrySetCanceled()))
                        {
                            return await blocked.Task;
                        }
                    });

                bool jobDispatched = false;
                slotA.Dispatcher
                    .Setup(x => x.Run(It.IsAny<Pipelines.AgentJobRequestMessage>(), false))
                    .Callback(() => jobDispatched = true);

                var listenerFactoryCalls = new Dictionary<string, int>();
                var coordinator = CreateCoordinator(hc, slots, listenerFactoryCalls);
                var runTask = coordinator.RunAsync();

                await WaitForAsync(() => jobDispatched, "job dispatched");
                slotB.Listener.Verify(x => x.DeleteSessionAsync(), Times.Once);

                // The raced job on repo B stays with the server: not deleted,
                // not acknowledged.
                slotB.Listener.Verify(x => x.DeleteMessageAsync(It.IsAny<TaskAgentMessage>()), Times.Never);
                slotB.Listener.Verify(x => x.AcknowledgeMessageAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
                slotB.Dispatcher.Verify(x => x.Run(It.IsAny<Pipelines.AgentJobRequestMessage>(), It.IsAny<bool>()), Times.Never);

                // On resume, repo B's listener is rebuilt so its message cursor
                // does not advertise the raced (unprocessed) message as consumed.
                slotA.Dispatcher.Raise(x => x.JobStatus += null, new JobStatusEventArgs(TaskAgentStatus.Online));
                await WaitForAsync(() => slotB.CreateSessionCalls >= 2, "repo B session recreated");
                lock (listenerFactoryCalls)
                {
                    Assert.Equal(2, listenerFactoryCalls["https://github.com/apfritts/repo-b"]);
                    Assert.Equal(1, listenerFactoryCalls["https://github.com/apfritts/repo-a"]);
                }

                hc.ShutdownRunner(ShutdownReason.UserCancelled);
                Assert.Equal(Constants.Runner.ReturnCode.Success, await runTask);
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Runner")]
        public async Task MigratedSettingsFallBackToOriginalOnSessionConflict()
        {
            var slotA = new TestSlot();
            var slots = new Dictionary<string, TestSlot>
            {
                ["https://github.com/apfritts/repo-a"] = slotA,
            };
            SeedRegistration("repo-a", "https://github.com/apfritts/repo-a", alsoMigrated: true);

            using (var hc = CreateTestContext(slots))
            {
                // First attempt (migrated settings) conflicts; the rebuilt slot
                // with original settings succeeds.
                slotA.Listener
                    .Setup(x => x.CreateSessionAsync(It.IsAny<CancellationToken>()))
                    .Returns(() =>
                    {
                        slotA.CreateSessionCalls++;
                        return Task.FromResult(slotA.CreateSessionCalls == 1
                            ? CreateSessionResult.SessionConflict
                            : CreateSessionResult.Success);
                    });
                SetupBlockingPoll(slotA);

                var listenerFactoryCalls = new Dictionary<string, int>();
                var coordinator = CreateCoordinator(hc, slots, listenerFactoryCalls);
                var runTask = coordinator.RunAsync();

                await WaitForAsync(() => slotA.CreateSessionCalls >= 2, "fallback session attempt");
                await WaitForAsync(() => slotA.PollCalls >= 1, "listening after fallback");
                lock (listenerFactoryCalls)
                {
                    Assert.Equal(2, listenerFactoryCalls["https://github.com/apfritts/repo-a"]);
                }

                hc.ShutdownRunner(ShutdownReason.UserCancelled);
                Assert.Equal(Constants.Runner.ReturnCode.Success, await runTask);
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Runner")]
        public async Task DeletedRegistrationIsRemovedLocallyWhileOthersKeepListening()
        {
            var slotA = new TestSlot();
            var slotB = new TestSlot();
            var slots = new Dictionary<string, TestSlot>
            {
                ["https://github.com/apfritts/repo-a"] = slotA,
                ["https://github.com/apfritts/repo-b"] = slotB,
            };
            SeedRegistration("repo-a", "https://github.com/apfritts/repo-a");
            SeedRegistration("repo-b", "https://github.com/apfritts/repo-b");

            using (var hc = CreateTestContext(slots))
            {
                SetupSessionSuccess(slotA);
                SetupSessionSuccess(slotB);
                SetupBlockingPoll(slotA);

                // Repo B's runner was deleted in the GitHub UI.
                slotB.Listener
                    .Setup(x => x.GetNextMessageAsync(It.IsAny<CancellationToken>()))
                    .Returns(() =>
                    {
                        slotB.PollCalls++;
                        return Task.FromException<TaskAgentMessage>(new RunnerNotFoundException("runner deleted"));
                    });

                var coordinator = CreateCoordinator(hc, slots);
                var runTask = coordinator.RunAsync();

                await WaitForAsync(
                    () => !Directory.Exists(Path.Combine(_tempRoot, Constants.MultiConfig.RegistrationsDirectory, "repo-b")),
                    "repo B local registration removed");
                Assert.True(Directory.Exists(Path.Combine(_tempRoot, Constants.MultiConfig.RegistrationsDirectory, "repo-a")));
                await WaitForAsync(() => slotA.PollCalls >= 1, "repo A still polling");

                hc.ShutdownRunner(ShutdownReason.UserCancelled);
                Assert.Equal(Constants.Runner.ReturnCode.Success, await runTask);

                // Only the dead registration's local config is gone; repo A's session
                // is deleted at shutdown, not before.
                slotA.Listener.Verify(x => x.DeleteSessionAsync(), Times.Once);
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Runner")]
        public async Task AllSessionConflictsAtStartupReturnsSessionConflict()
        {
            var slotA = new TestSlot();
            var slotB = new TestSlot();
            var slots = new Dictionary<string, TestSlot>
            {
                ["https://github.com/apfritts/repo-a"] = slotA,
                ["https://github.com/apfritts/repo-b"] = slotB,
            };
            SeedRegistration("repo-a", "https://github.com/apfritts/repo-a");
            SeedRegistration("repo-b", "https://github.com/apfritts/repo-b");

            using (var hc = CreateTestContext(slots))
            {
                slotA.Listener.Setup(x => x.CreateSessionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(CreateSessionResult.SessionConflict);
                slotB.Listener.Setup(x => x.CreateSessionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(CreateSessionResult.SessionConflict);

                var coordinator = CreateCoordinator(hc, slots);

                Assert.Equal(Constants.Runner.ReturnCode.SessionConflict, await coordinator.RunAsync());
                slotA.Listener.Verify(x => x.GetNextMessageAsync(It.IsAny<CancellationToken>()), Times.Never);
            }
        }
    }
}
