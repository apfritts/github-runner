using System;
using System.IO;
using System.Linq;
using GitHub.Runner.Sdk;
using Xunit;

namespace GitHub.Runner.Common.Tests
{
    public sealed class RegistrationStoreL0
    {
        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Common")]
        public void NormalizeGitHubUrlHandlesVariants()
        {
            Assert.Equal("https://github.com/apfritts/repo-1", RegistrationStore.NormalizeGitHubUrl("https://github.com/apfritts/repo-1"));
            Assert.Equal("https://github.com/apfritts/repo-1", RegistrationStore.NormalizeGitHubUrl("https://GitHub.com/Apfritts/Repo-1/"));
            Assert.Equal("https://github.com/apfritts/repo-1", RegistrationStore.NormalizeGitHubUrl("https://github.com/apfritts/repo-1.git"));
            Assert.Equal("https://ghes.example.com:8443/org/repo", RegistrationStore.NormalizeGitHubUrl("https://ghes.example.com:8443/org/repo/"));
            Assert.NotEqual(
                RegistrationStore.NormalizeGitHubUrl("https://github.com/apfritts/repo-1"),
                RegistrationStore.NormalizeGitHubUrl("https://github.com/apfritts/repo-2"));
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Common")]
        public void MakeSlugSanitizesUrls()
        {
            Assert.Equal("apfritts-repo-1", RegistrationStore.MakeSlug("https://github.com/apfritts/repo-1"));
            Assert.Equal("apfritts-repo-1", RegistrationStore.MakeSlug("https://github.com/Apfritts/Repo-1/"));
            Assert.Equal("myorg", RegistrationStore.MakeSlug("https://github.com/myorg"));
            Assert.Equal("org-my-20repo", RegistrationStore.MakeSlug("https://github.com/org/my%20repo"));
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Common")]
        public void CreateSlugDirectoryDisambiguatesCollisions()
        {
            using (TestHostContext hc = new(this))
            using (var root = new TempRoot())
            {
                var store = CreateStore(hc, root);

                var first = store.CreateSlugDirectory("https://github.com/apfritts/repo-1");
                Assert.Equal("apfritts-repo-1", first.Slug);
                Assert.True(Directory.Exists(first.Directory));

                // Occupy the first directory, then configure a URL that sanitizes
                // to the same slug.
                WriteSettings(first.Directory, "https://github.com/apfritts/repo-1");
                var second = store.CreateSlugDirectory("https://github.com/apfritts/repo+1");
                Assert.StartsWith("apfritts-repo-1-", second.Slug);
                Assert.NotEqual(first.Slug, second.Slug);
                Assert.True(Directory.Exists(second.Directory));
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Common")]
        public void GetAllFallsBackToLegacyFlatConfig()
        {
            using (TestHostContext hc = new(this))
            using (var root = new TempRoot())
            {
                var store = CreateStore(hc, root);
                WriteSettings(root.Path, "https://github.com/apfritts/legacy-repo");

                var registrations = store.GetAll();

                Assert.Equal(1, registrations.Count);
                Assert.True(registrations[0].IsLegacy);
                Assert.Null(registrations[0].Slug);
                Assert.Equal(root.Path, registrations[0].Directory);
                Assert.Equal("https://github.com/apfritts/legacy-repo", registrations[0].GitHubUrl);
                Assert.False(store.IsMultiLayout());
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Common")]
        public void GetAllAndFindWorkForMultiLayout()
        {
            using (TestHostContext hc = new(this))
            using (var root = new TempRoot())
            {
                var store = CreateStore(hc, root);
                var first = store.CreateSlugDirectory("https://github.com/apfritts/repo-1");
                WriteSettings(first.Directory, "https://github.com/apfritts/repo-1");
                var second = store.CreateSlugDirectory("https://github.com/apfritts/repo-2");
                WriteSettings(second.Directory, "https://github.com/apfritts/repo-2");

                var registrations = store.GetAll();

                Assert.Equal(2, registrations.Count);
                Assert.All(registrations, r => Assert.False(r.IsLegacy));
                Assert.Equal(new[] { "apfritts-repo-1", "apfritts-repo-2" }, registrations.Select(r => r.Slug).ToArray());
                Assert.True(store.IsMultiLayout());

                // Find normalizes before comparing.
                Assert.Equal("apfritts-repo-2", store.Find("https://GitHub.com/Apfritts/Repo-2/").Slug);
                Assert.Null(store.Find("https://github.com/apfritts/other-repo"));
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Common")]
        public void MigrateLegacyToSlugMovesRegistrationFiles()
        {
            using (TestHostContext hc = new(this))
            using (var root = new TempRoot())
            {
                var store = CreateStore(hc, root);
                WriteSettings(root.Path, "https://github.com/apfritts/legacy-repo");
                File.WriteAllText(Path.Combine(root.Path, ".credentials"), "{\"scheme\":\"OAuth\"}");
                File.WriteAllText(Path.Combine(root.Path, ".credentials_rsaparams"), "{}");

                var migrated = store.MigrateLegacyToSlug();

                Assert.Equal("apfritts-legacy-repo", migrated.Slug);
                Assert.True(File.Exists(Path.Combine(migrated.Directory, ".runner")));
                Assert.True(File.Exists(Path.Combine(migrated.Directory, ".credentials")));
                Assert.True(File.Exists(Path.Combine(migrated.Directory, ".credentials_rsaparams")));
                Assert.False(File.Exists(Path.Combine(root.Path, ".runner")));
                Assert.False(File.Exists(Path.Combine(root.Path, ".credentials")));
                Assert.False(File.Exists(Path.Combine(root.Path, ".credentials_rsaparams")));

                var registrations = store.GetAll();
                Assert.Equal(1, registrations.Count);
                Assert.False(registrations[0].IsLegacy);
                Assert.Equal("https://github.com/apfritts/legacy-repo", registrations[0].GitHubUrl);
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Common")]
        public void PartialSlugDirectoryDoesNotHideLegacyConfig()
        {
            using (TestHostContext hc = new(this))
            using (var root = new TempRoot())
            {
                var store = CreateStore(hc, root);
                WriteSettings(root.Path, "https://github.com/apfritts/legacy-repo");

                // A crashed migration leaves credentials but no .runner marker;
                // such a directory must not shadow the intact legacy layout.
                var partialDir = Path.Combine(root.Path, Constants.MultiConfig.RegistrationsDirectory, "broken");
                Directory.CreateDirectory(partialDir);
                File.WriteAllText(Path.Combine(partialDir, ".credentials"), "{\"scheme\":\"OAuth\"}");

                var registrations = store.GetAll();

                Assert.Equal(1, registrations.Count);
                Assert.True(registrations[0].IsLegacy);
                Assert.Equal("https://github.com/apfritts/legacy-repo", registrations[0].GitHubUrl);
                Assert.False(store.IsMultiLayout());
            }
        }

        [Fact]
        [Trait("Level", "L0")]
        [Trait("Category", "Common")]
        public void DeleteRegistrationRemovesDirectoryAndEmptyRegistrationsRoot()
        {
            using (TestHostContext hc = new(this))
            using (var root = new TempRoot())
            {
                var store = CreateStore(hc, root);
                var first = store.CreateSlugDirectory("https://github.com/apfritts/repo-1");
                WriteSettings(first.Directory, "https://github.com/apfritts/repo-1");
                var second = store.CreateSlugDirectory("https://github.com/apfritts/repo-2");
                WriteSettings(second.Directory, "https://github.com/apfritts/repo-2");

                store.DeleteRegistration(first.Slug);

                Assert.False(Directory.Exists(first.Directory));
                Assert.True(Directory.Exists(second.Directory));
                Assert.True(store.IsMultiLayout());

                store.DeleteRegistration(second.Slug);

                Assert.False(Directory.Exists(Path.Combine(root.Path, Constants.MultiConfig.RegistrationsDirectory)));
                Assert.False(store.IsMultiLayout());
            }
        }

        private RegistrationStore CreateStore(TestHostContext hc, TempRoot root)
        {
            var store = new RegistrationStore();
            store.Initialize(hc);
            store.RootDirectory = root.Path;
            return store;
        }

        private static void WriteSettings(string directory, string gitHubUrl)
        {
            IOUtil.SaveObject(
                new RunnerSettings
                {
                    AgentId = 1,
                    AgentName = "test-runner",
                    GitHubUrl = gitHubUrl,
                    WorkFolder = "_work",
                },
                Path.Combine(directory, ".runner"));
        }

        private sealed class TempRoot : IDisposable
        {
            public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), Guid.NewGuid().ToString("D"));

            public TempRoot()
            {
                Directory.CreateDirectory(Path);
            }

            public void Dispose()
            {
                try
                {
                    Directory.Delete(Path, recursive: true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }
    }
}
