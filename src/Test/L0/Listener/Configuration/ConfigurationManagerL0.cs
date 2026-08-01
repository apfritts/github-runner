using GitHub.DistributedTask.WebApi;
using GitHub.Runner.Listener;
using GitHub.Runner.Listener.Configuration;
using GitHub.Runner.Common.Util;
using GitHub.Services.WebApi;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Xunit;
using GitHub.Services.Location;
using GitHub.Services.Common;

namespace GitHub.Runner.Common.Tests.Listener.Configuration
{
    public class ConfigurationManagerL0 : IDisposable
    {
        private Mock<IRunnerServer> _runnerServer;
        private Mock<IRunnerDotcomServer> _dotcomServer;
        private Mock<ILocationServer> _locationServer;
        private Mock<ICredentialManager> _credMgr;
        private Mock<IPromptManager> _promptManager;
        private Mock<IConfigurationStore> _store;
        private Mock<IExtensionManager> _extnMgr;

#if OS_WINDOWS
        private Mock<IWindowsServiceControlManager> _serviceControlManager;
#endif

#if !OS_WINDOWS
        private Mock<ILinuxServiceControlManager> _serviceControlManager;
#endif

        private Mock<IRSAKeyManager> _rsaKeyManager;
        private RegistrationStore _registrationStore;
        private string _tempRoot;
        private string _expectedToken = "expectedToken";
        private string _expectedServerUrl = "https://codedev.ms";
        private string _expectedAgentName = "expectedAgentName";
        private string _defaultRunnerGroupName = "defaultRunnerGroup";
        private string _secondRunnerGroupName = "secondRunnerGroup";
        private string _expectedAuthType = "pat";
        private string _expectedWorkFolder = "_work";
        private int _defaultRunnerGroupId = 1;
        private int _secondRunnerGroupId = 2;
        private RSACryptoServiceProvider rsa = null;
        private RunnerSettings _configMgrAgentSettings = new();

        public ConfigurationManagerL0()
        {
            _runnerServer = new Mock<IRunnerServer>();
            _locationServer = new Mock<ILocationServer>();
            _credMgr = new Mock<ICredentialManager>();
            _promptManager = new Mock<IPromptManager>();
            _store = new Mock<IConfigurationStore>();
            _extnMgr = new Mock<IExtensionManager>();
            _rsaKeyManager = new Mock<IRSAKeyManager>();
            _dotcomServer = new Mock<IRunnerDotcomServer>();

#if OS_WINDOWS
            _serviceControlManager = new Mock<IWindowsServiceControlManager>();
#endif

#if !OS_WINDOWS
            _serviceControlManager = new Mock<ILinuxServiceControlManager>();
#endif

            var expectedAgent = new TaskAgent(_expectedAgentName) { Id = 1, Ephemeral = true, DisableUpdate = true };
            expectedAgent.Authorization = new TaskAgentAuthorization
            {
                ClientId = Guid.NewGuid(),
                AuthorizationUrl = new Uri("http://localhost:8080/pipelines"),
            };

            var expectedRunner = new GitHub.DistributedTask.WebApi.Runner() { Name = expectedAgent.Name, Id = 1 };
            expectedRunner.RunnerAuthorization = new GitHub.DistributedTask.WebApi.Runner.Authorization
            {
                ClientId = expectedAgent.Authorization.ClientId.ToString(),
                AuthorizationUrl = new Uri("http://localhost:8080/pipelines"),
            };

            var connectionData = new ConnectionData()
            {
                InstanceId = Guid.NewGuid(),
                DeploymentType = DeploymentFlags.Hosted,
                DeploymentId = Guid.NewGuid()
            };
            _runnerServer.Setup(x => x.ConnectAsync(It.IsAny<Uri>(), It.IsAny<VssCredentials>())).Returns(Task.FromResult<object>(null));
            _locationServer.Setup(x => x.ConnectAsync(It.IsAny<VssConnection>())).Returns(Task.FromResult<object>(null));
            _locationServer.Setup(x => x.GetConnectionDataAsync()).Returns(Task.FromResult<ConnectionData>(connectionData));

            _store.Setup(x => x.IsConfigured()).Returns(false);
            _store.Setup(x => x.HasCredentials()).Returns(false);
            _store.Setup(x => x.GetSettings()).Returns(() => _configMgrAgentSettings);

            _store.Setup(x => x.SaveSettings(It.IsAny<RunnerSettings>()))
                .Callback((RunnerSettings settings) =>
                {
                    _configMgrAgentSettings = settings;
                });

            _credMgr.Setup(x => x.GetCredentialProvider(It.IsAny<string>())).Returns(new TestRunnerCredential());

#if !OS_WINDOWS
            _serviceControlManager.Setup(x => x.GenerateScripts(It.IsAny<RunnerSettings>()));
#endif

            var expectedPools = new List<TaskAgentPool>() { new TaskAgentPool(_defaultRunnerGroupName) { Id = _defaultRunnerGroupId, IsInternal = true }, new TaskAgentPool(_secondRunnerGroupName) { Id = _secondRunnerGroupId } };
            _runnerServer.Setup(x => x.GetAgentPoolsAsync(It.IsAny<string>(), It.IsAny<TaskAgentPoolType>())).Returns(Task.FromResult(expectedPools));

            var expectedAgents = new List<TaskAgent>();
            _runnerServer.Setup(x => x.GetAgentsAsync(It.IsAny<string>())).Returns(Task.FromResult(expectedAgents));

            _runnerServer.Setup(x => x.AddAgentAsync(It.IsAny<int>(), It.IsAny<TaskAgent>())).Returns(Task.FromResult(expectedAgent));
            _runnerServer.Setup(x => x.ReplaceAgentAsync(It.IsAny<int>(), It.IsAny<TaskAgent>())).Returns(Task.FromResult(expectedAgent));

            _dotcomServer.Setup(x => x.GetRunnerByNameAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>())).Returns(Task.FromResult(expectedAgents));
            _dotcomServer.Setup(x => x.GetRunnerGroupsAsync(It.IsAny<string>(), It.IsAny<string>())).Returns(Task.FromResult(expectedPools));
            _dotcomServer.Setup(x => x.AddRunnerAsync(It.IsAny<int>(), It.IsAny<TaskAgent>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>())).Returns(Task.FromResult(expectedRunner));

            rsa = new RSACryptoServiceProvider(2048);

            _rsaKeyManager.Setup(x => x.CreateKey()).Returns(rsa);

            // Real registration store rooted at a per-test temp directory so
            // multi-repository layouts can be seeded on disk.
            _tempRoot = System.IO.Path.Combine(System.IO.Path.GetTempPath(), Guid.NewGuid().ToString("D"));
            System.IO.Directory.CreateDirectory(_tempRoot);
            _registrationStore = new RegistrationStore
            {
                RootDirectory = _tempRoot,
            };
        }

        public void Dispose()
        {
            try
            {
                System.IO.Directory.Delete(_tempRoot, recursive: true);
            }
            catch (System.IO.IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        private TestHostContext CreateTestContext([CallerMemberName] String testName = "")
        {
            TestHostContext tc = new(this, testName);
            tc.SetSingleton<ICredentialManager>(_credMgr.Object);
            tc.SetSingleton<IPromptManager>(_promptManager.Object);
            tc.SetSingleton<IConfigurationStore>(_store.Object);
            tc.SetSingleton<IExtensionManager>(_extnMgr.Object);
            tc.SetSingleton<IRunnerServer>(_runnerServer.Object);
            tc.SetSingleton<IRunnerDotcomServer>(_dotcomServer.Object);
            tc.SetSingleton<ILocationServer>(_locationServer.Object);

#if OS_WINDOWS
            tc.SetSingleton<IWindowsServiceControlManager>(_serviceControlManager.Object);
#else
            tc.SetSingleton<ILinuxServiceControlManager>(_serviceControlManager.Object);
#endif

            tc.SetSingleton<IRSAKeyManager>(_rsaKeyManager.Object);
            tc.SetSingleton<IRegistrationStore>(_registrationStore);

            return tc;
        }

        private string SeedRegistration(string slug, string gitHubUrl, bool useRunnerAdminFlow = false, ulong agentId = 1, bool legacy = false, bool ephemeral = false)
        {
            var dir = legacy ? _tempRoot : System.IO.Path.Combine(_tempRoot, Constants.MultiConfig.RegistrationsDirectory, slug);
            System.IO.Directory.CreateDirectory(dir);
            GitHub.Runner.Sdk.IOUtil.SaveObject(
                new RunnerSettings
                {
                    AgentId = agentId,
                    AgentName = _expectedAgentName,
                    GitHubUrl = gitHubUrl,
                    ServerUrl = _expectedServerUrl,
                    WorkFolder = _expectedWorkFolder,
                    UseRunnerAdminFlow = useRunnerAdminFlow,
                    Ephemeral = ephemeral,
                },
                System.IO.Path.Combine(dir, ".runner"));
            System.IO.File.WriteAllText(System.IO.Path.Combine(dir, ".credentials"), "{\"scheme\":\"OAuth\"}");
            return dir;
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task CanEnsureConfigure()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                Tracing trace = tc.GetTrace();

                trace.Info("Creating config manager");
                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);

                var userLabels = "userlabel1,userlabel2";

                trace.Info("Preparing command line arguments");
                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "configure",
                       "--url", _expectedServerUrl,
                       "--name", _expectedAgentName,
                       "--runnergroup", _secondRunnerGroupName,
                       "--work", _expectedWorkFolder,
                       "--auth", _expectedAuthType,
                       "--token", _expectedToken,
                       "--labels", userLabels,
                       "--ephemeral",
                       "--disableupdate",
                       "--unattended",
                    });
                trace.Info("Constructed.");
                _store.Setup(x => x.IsConfigured()).Returns(false);
                _configMgrAgentSettings = null;

                trace.Info("Ensuring all the required parameters are available in the command line parameter");
                await configManager.ConfigureAsync(command);

                _store.Setup(x => x.IsConfigured()).Returns(true);

                trace.Info("Configured, verifying all the parameter value");
                var s = configManager.LoadSettings();
                Assert.NotNull(s);
                Assert.Equal(_expectedServerUrl, s.ServerUrl);
                Assert.Equal(_expectedAgentName, s.AgentName);
                Assert.Equal(_secondRunnerGroupId, s.PoolId);
                Assert.Equal(_expectedWorkFolder, s.WorkFolder);
                Assert.True(s.Ephemeral);

                // validate GetAgentPoolsAsync gets called twice with automation pool type
                _runnerServer.Verify(x => x.GetAgentPoolsAsync(It.IsAny<string>(), It.Is<TaskAgentPoolType>(p => p == TaskAgentPoolType.Automation)), Times.Exactly(2));

                var expectedLabels = new List<string>() { "self-hosted", VarUtil.OS, VarUtil.OSArchitecture };
                expectedLabels.AddRange(userLabels.Split(",").ToList());

                _runnerServer.Verify(x => x.AddAgentAsync(It.IsAny<int>(), It.Is<TaskAgent>(a => a.Labels.Select(x => x.Name).ToHashSet().SetEquals(expectedLabels))), Times.Once);
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task ConfigureErrorDefaultLabelsDisabledWithNoCustomLabels()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                Tracing trace = tc.GetTrace();

                trace.Info("Creating config manager");
                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);

                trace.Info("Preparing command line arguments");
                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "configure",
                       "--url", _expectedServerUrl,
                       "--name", _expectedAgentName,
                       "--runnergroup", _secondRunnerGroupName,
                       "--work", _expectedWorkFolder,
                       "--auth", _expectedAuthType,
                       "--token", _expectedToken,
                       "--no-default-labels",
                       "--ephemeral",
                       "--disableupdate",
                       "--unattended",
                    });
                trace.Info("Constructed.");
                _store.Setup(x => x.IsConfigured()).Returns(false);
                _configMgrAgentSettings = null;

                trace.Info("Ensuring configure fails if default labels are disabled and no custom labels are set");
                var ex = await Assert.ThrowsAsync<NotSupportedException>(() => configManager.ConfigureAsync(command));

                Assert.Contains("--no-default-labels without specifying --labels is not supported", ex.Message);
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task ConfigureDefaultLabelsDisabledWithCustomLabels()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                Tracing trace = tc.GetTrace();

                trace.Info("Creating config manager");
                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);

                var userLabels = "userlabel1,userlabel2";

                trace.Info("Preparing command line arguments");
                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "configure",
                       "--url", _expectedServerUrl,
                       "--name", _expectedAgentName,
                       "--runnergroup", _secondRunnerGroupName,
                       "--work", _expectedWorkFolder,
                       "--auth", _expectedAuthType,
                       "--token", _expectedToken,
                       "--labels", userLabels,
                       "--no-default-labels",
                       "--ephemeral",
                       "--disableupdate",
                       "--unattended",
                    });
                trace.Info("Constructed.");
                _store.Setup(x => x.IsConfigured()).Returns(false);
                _configMgrAgentSettings = null;

                trace.Info("Ensuring all the required parameters are available in the command line parameter");
                await configManager.ConfigureAsync(command);

                _store.Setup(x => x.IsConfigured()).Returns(true);

                trace.Info("Configured, verifying all the parameter value");
                var s = configManager.LoadSettings();
                Assert.NotNull(s);
                Assert.Equal(_expectedServerUrl, s.ServerUrl);
                Assert.Equal(_expectedAgentName, s.AgentName);
                Assert.Equal(_secondRunnerGroupId, s.PoolId);
                Assert.Equal(_expectedWorkFolder, s.WorkFolder);
                Assert.True(s.Ephemeral);

                // validate GetAgentPoolsAsync gets called twice with automation pool type
                _runnerServer.Verify(x => x.GetAgentPoolsAsync(It.IsAny<string>(), It.Is<TaskAgentPoolType>(p => p == TaskAgentPoolType.Automation)), Times.Exactly(2));

                var expectedLabels = userLabels.Split(",").ToList();

                _runnerServer.Verify(x => x.AddAgentAsync(It.IsAny<int>(), It.Is<TaskAgent>(a => a.Labels.Select(x => x.Name).ToHashSet().SetEquals(expectedLabels))), Times.Once);
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task ConfigureErrorOnMissingRunnerGroup()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                var expectedPools = new List<TaskAgentPool>() { new TaskAgentPool(_defaultRunnerGroupName) { Id = _defaultRunnerGroupId, IsInternal = true } };
                _runnerServer.Setup(x => x.GetAgentPoolsAsync(It.IsAny<string>(), It.IsAny<TaskAgentPoolType>())).Returns(Task.FromResult(expectedPools));

                Tracing trace = tc.GetTrace();

                trace.Info("Creating config manager");
                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);


                trace.Info("Preparing command line arguments");
                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "configure",
                       "--url", _expectedServerUrl,
                       "--name", _expectedAgentName,
                       "--runnergroup", "notexists",
                       "--work", _expectedWorkFolder,
                       "--auth", _expectedAuthType,
                       "--token", _expectedToken,
                    });
                trace.Info("Constructed.");
                _store.Setup(x => x.IsConfigured()).Returns(false);
                _configMgrAgentSettings = null;

                trace.Info("Ensuring all the required parameters are available in the command line parameter");
                var ex = await Assert.ThrowsAsync<TaskAgentPoolNotFoundException>(() => configManager.ConfigureAsync(command));

                Assert.Contains("notexists", ex.Message);

                _runnerServer.Verify(x => x.GetAgentPoolsAsync(It.IsAny<string>(), It.Is<TaskAgentPoolType>(p => p == TaskAgentPoolType.Automation)), Times.Exactly(1));
            }
        }

#if OS_LINUX
        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task ConfigureRunnerServiceFailsOnUnconfiguredRunners()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                Tracing trace = tc.GetTrace();

                trace.Info("Creating config manager");
                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);

                trace.Info("Preparing command line arguments");
                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "configure",
                       "--generateServiceConfig",
                    });
                trace.Info("Constructed");
                _store.Setup(x => x.IsConfigured()).Returns(false);

                trace.Info("Ensuring service generation mode fails when on un-configured runners");
                var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => configManager.ConfigureAsync(command));

                Assert.Contains("requires that the runner is already configured", ex.Message);
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task ConfigureRunnerServiceCreatesService()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                Tracing trace = tc.GetTrace();

                trace.Info("Creating config manager");
                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);

                trace.Info("Preparing command line arguments");
                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "configure",
                       "--generateServiceConfig",
                    });
                trace.Info("Constructed");

                _store.Setup(x => x.IsConfigured()).Returns(true);

                trace.Info("Ensuring service generation mode fails when on un-configured runners");
                await configManager.ConfigureAsync(command);

                _serviceControlManager.Verify(x => x.GenerateScripts(It.IsAny<RunnerSettings>()), Times.Once);
            }
        }
#endif

#if !OS_LINUX
        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task ConfigureRunnerServiceFailsOnUnsupportedPlatforms()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                Tracing trace = tc.GetTrace();

                trace.Info("Creating config manager");
                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);

                trace.Info("Preparing command line arguments");
                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "configure",
                       "--generateServiceConfig",
                    });
                trace.Info("Constructed");
                _store.Setup(x => x.IsConfigured()).Returns(true);

                trace.Info("Ensuring service generation mode fails on unsupported runner platforms");
                var ex = await Assert.ThrowsAsync<NotSupportedException>(() => configManager.ConfigureAsync(command));

                Assert.Contains("only supported on Linux", ex.Message);
            }
        }
#endif

#if !OS_WINDOWS
        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task ConfigureSecondRepositoryAddsRegistration()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                SeedRegistration("first", "https://codedev.ms/org-one");
                tc.EnqueueInstance<IConfigurationStore>(new ConfigurationStore());
                tc.EnqueueInstance<IConfigurationStore>(new ConfigurationStore());
                var slugKeyManager = new Mock<IRSAKeyManager>();
                slugKeyManager.SetupAllProperties();
                slugKeyManager.Setup(x => x.CreateKey()).Returns(rsa);
                tc.EnqueueInstance<IRSAKeyManager>(slugKeyManager.Object);

                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);

                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "configure",
                       "--url", _expectedServerUrl,
                       "--name", _expectedAgentName,
                       "--runnergroup", _secondRunnerGroupName,
                       "--work", _expectedWorkFolder,
                       "--auth", _expectedAuthType,
                       "--token", _expectedToken,
                       "--labels", "userlabel1",
                       "--unattended",
                    });

                await configManager.ConfigureAsync(command);

                // The new registration lands in its own slug directory with its
                // own settings/credentials; the flat layout stays untouched.
                var newDir = System.IO.Path.Combine(_tempRoot, Constants.MultiConfig.RegistrationsDirectory, "codedev.ms");
                Assert.True(System.IO.File.Exists(System.IO.Path.Combine(newDir, ".runner")));
                Assert.True(System.IO.File.Exists(System.IO.Path.Combine(newDir, ".credentials")));
                var savedSettings = GitHub.Runner.Sdk.IOUtil.LoadObject<RunnerSettings>(System.IO.Path.Combine(newDir, ".runner"));
                Assert.True(savedSettings.DisableUpdate);
                Assert.Equal(_expectedWorkFolder, savedSettings.WorkFolder);
                Assert.NotNull(slugKeyManager.Object.KeyFileOverride);
                _store.Verify(x => x.SaveSettings(It.IsAny<RunnerSettings>()), Times.Never);
                _store.Verify(x => x.SaveCredential(It.IsAny<CredentialData>()), Times.Never);
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task ConfigureDuplicateRepositoryFails()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                SeedRegistration("first", _expectedServerUrl);
                tc.EnqueueInstance<IConfigurationStore>(new ConfigurationStore());

                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);

                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "configure",
                       "--url", _expectedServerUrl,
                       "--name", _expectedAgentName,
                       "--auth", _expectedAuthType,
                       "--token", _expectedToken,
                       "--unattended",
                    });

                var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => configManager.ConfigureAsync(command));

                Assert.Contains("already listening", ex.Message);
                Assert.False(System.IO.Directory.Exists(System.IO.Path.Combine(_tempRoot, Constants.MultiConfig.RegistrationsDirectory, "codedev.ms")));
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task ConfigureEphemeralRejectedWhenAddingRegistration()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                SeedRegistration("first", "https://codedev.ms/org-one");

                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);

                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "configure",
                       "--url", _expectedServerUrl,
                       "--token", _expectedToken,
                       "--ephemeral",
                       "--unattended",
                    });

                var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => configManager.ConfigureAsync(command));

                Assert.Contains("--ephemeral", ex.Message);
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task ConfigureMigratesLegacyLayoutWhenAddingRegistration()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                SeedRegistration(null, "https://codedev.ms/legacy-repo", legacy: true);
                tc.EnqueueInstance<IConfigurationStore>(new ConfigurationStore());
                tc.EnqueueInstance<IConfigurationStore>(new ConfigurationStore());
                tc.EnqueueInstance<IConfigurationStore>(new ConfigurationStore());
                var slugKeyManager = new Mock<IRSAKeyManager>();
                slugKeyManager.Setup(x => x.CreateKey()).Returns(rsa);
                tc.EnqueueInstance<IRSAKeyManager>(slugKeyManager.Object);

                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);

                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "configure",
                       "--url", _expectedServerUrl,
                       "--name", _expectedAgentName,
                       "--runnergroup", _secondRunnerGroupName,
                       "--work", _expectedWorkFolder,
                       "--auth", _expectedAuthType,
                       "--token", _expectedToken,
                       "--unattended",
                    });

                await configManager.ConfigureAsync(command);

                // Legacy flat files moved into .runners/legacy-repo/ with updates
                // disabled; the new registration got its own directory.
                Assert.False(System.IO.File.Exists(System.IO.Path.Combine(_tempRoot, ".runner")));
                var migratedDir = System.IO.Path.Combine(_tempRoot, Constants.MultiConfig.RegistrationsDirectory, "legacy-repo");
                var migratedSettings = GitHub.Runner.Sdk.IOUtil.LoadObject<RunnerSettings>(System.IO.Path.Combine(migratedDir, ".runner"));
                Assert.True(migratedSettings.DisableUpdate);
                Assert.True(System.IO.File.Exists(System.IO.Path.Combine(migratedDir, ".credentials")));
                var newDir = System.IO.Path.Combine(_tempRoot, Constants.MultiConfig.RegistrationsDirectory, "codedev.ms");
                Assert.True(System.IO.File.Exists(System.IO.Path.Combine(newDir, ".runner")));
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task RemoveWithUrlSelectorRemovesOnlyThatRegistration()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                SeedRegistration("org-a", "https://codedev.ms/org-a", useRunnerAdminFlow: true, agentId: 11);
                SeedRegistration("org-b", "https://codedev.ms/org-b", useRunnerAdminFlow: true, agentId: 22);
                tc.EnqueueInstance<IConfigurationStore>(new ConfigurationStore());
                var slugKeyManager = new Mock<IRSAKeyManager>();
                tc.EnqueueInstance<IRSAKeyManager>(slugKeyManager.Object);

                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);

                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "remove",
                       "--url", "https://codedev.ms/org-b",
                       "--token", _expectedToken,
                       "--unattended",
                    });

                await configManager.UnconfigureAsync(command);

                Assert.False(System.IO.Directory.Exists(System.IO.Path.Combine(_tempRoot, Constants.MultiConfig.RegistrationsDirectory, "org-b")));
                Assert.True(System.IO.Directory.Exists(System.IO.Path.Combine(_tempRoot, Constants.MultiConfig.RegistrationsDirectory, "org-a")));
                _dotcomServer.Verify(x => x.DeleteRunnerAsync("https://codedev.ms/org-b", _expectedToken, 22), Times.Once);
                slugKeyManager.Verify(x => x.DeleteKey(), Times.Once);
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "ConfigurationManagement")]
        public async Task RemoveWithoutUrlSelectorFailsWithMultipleRegistrations()
        {
            using (TestHostContext tc = CreateTestContext())
            {
                SeedRegistration("org-a", "https://codedev.ms/org-a", useRunnerAdminFlow: true, agentId: 11);
                SeedRegistration("org-b", "https://codedev.ms/org-b", useRunnerAdminFlow: true, agentId: 22);

                IConfigurationManager configManager = new ConfigurationManager();
                configManager.Initialize(tc);

                var command = new CommandSettings(
                    tc,
                    new[]
                    {
                       "remove",
                       "--token", _expectedToken,
                       "--unattended",
                    });

                var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => configManager.UnconfigureAsync(command));

                Assert.Contains("--url", ex.Message);
                Assert.Contains("https://codedev.ms/org-a", ex.Message);
                Assert.Contains("https://codedev.ms/org-b", ex.Message);
                Assert.True(System.IO.Directory.Exists(System.IO.Path.Combine(_tempRoot, Constants.MultiConfig.RegistrationsDirectory, "org-a")));
                Assert.True(System.IO.Directory.Exists(System.IO.Path.Combine(_tempRoot, Constants.MultiConfig.RegistrationsDirectory, "org-b")));
            }
        }
#endif
    }
}
