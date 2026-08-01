using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using GitHub.Runner.Sdk;

namespace GitHub.Runner.Common
{
    // One configured repository registration inside a runner install directory.
    // In the multi-repository layout each registration lives under
    // <root>/.runners/<slug>/; a runner configured the classic way is surfaced
    // as a single legacy registration rooted at <root>.
    public sealed class RegistrationRef
    {
        public string Slug { get; set; }
        public string Directory { get; set; }
        public string GitHubUrl { get; set; }
        public bool IsLegacy { get; set; }
    }

    [ServiceLocator(Default = typeof(RegistrationStore))]
    public interface IRegistrationStore : IRunnerService
    {
        IReadOnlyList<RegistrationRef> GetAll();
        bool IsMultiLayout();
        RegistrationRef Find(string gitHubUrl);
        RegistrationRef CreateSlugDirectory(string gitHubUrl);
        void DeleteRegistration(string slug);
        RegistrationRef MigrateLegacyToSlug();
    }

    public sealed class RegistrationStore : RunnerService, IRegistrationStore
    {
        // Per-registration files that move between the legacy flat layout and
        // a .runners/<slug>/ directory. The RSA key file must stay owner-only.
        private static readonly string[] RegistrationFileNames =
        {
            ".runner",
            ".runner_migrated",
            ".credentials",
            ".credentials_migrated",
            ".credentials_rsaparams",
        };

        public string RootDirectory { get; set; }

        public override void Initialize(IHostContext hostContext)
        {
            base.Initialize(hostContext);
            if (string.IsNullOrEmpty(RootDirectory))
            {
                RootDirectory = hostContext.GetDirectory(WellKnownDirectory.Root);
            }
        }

        public IReadOnlyList<RegistrationRef> GetAll()
        {
            var registrations = new List<RegistrationRef>();
            foreach (var directory in EnumerateRegistrationDirectories())
            {
                var settings = TryLoadSettings(directory);
                if (settings == null)
                {
                    Trace.Warning($"Skipping registration directory with unreadable settings: {directory}");
                    continue;
                }

                registrations.Add(new RegistrationRef
                {
                    Slug = Path.GetFileName(directory),
                    Directory = directory,
                    GitHubUrl = settings.GitHubUrl,
                    IsLegacy = false,
                });
            }

            if (registrations.Count > 0)
            {
                if (HasLegacyConfig())
                {
                    Trace.Warning("Both a legacy flat runner config and .runners registrations exist; the legacy config is ignored.");
                }

                return registrations;
            }

            var legacySettings = TryLoadSettings(RootDirectory);
            if (legacySettings != null)
            {
                registrations.Add(new RegistrationRef
                {
                    Slug = null,
                    Directory = RootDirectory,
                    GitHubUrl = legacySettings.GitHubUrl,
                    IsLegacy = true,
                });
            }

            return registrations;
        }

        public bool IsMultiLayout()
        {
            return EnumerateRegistrationDirectories().Any();
        }

        public RegistrationRef Find(string gitHubUrl)
        {
            var normalized = NormalizeGitHubUrl(gitHubUrl);
            return GetAll().FirstOrDefault(registration =>
                !string.IsNullOrEmpty(registration.GitHubUrl) &&
                string.Equals(NormalizeGitHubUrl(registration.GitHubUrl), normalized, StringComparison.Ordinal));
        }

        public RegistrationRef CreateSlugDirectory(string gitHubUrl)
        {
            var normalized = NormalizeGitHubUrl(gitHubUrl);
            var slug = MakeSlug(gitHubUrl);
            var registrationsRoot = Path.Combine(RootDirectory, Constants.MultiConfig.RegistrationsDirectory);
            var directory = Path.Combine(registrationsRoot, slug);
            if (IsOccupied(directory))
            {
                // Two different URLs can sanitize to the same slug; disambiguate
                // deterministically so remove can find the directory again.
                slug = $"{slug}-{IOUtil.GetSha256Hash(normalized).Substring(0, 6)}";
                directory = Path.Combine(registrationsRoot, slug);
                if (IsOccupied(directory))
                {
                    throw new InvalidOperationException($"Registration directory '{directory}' already contains a configured runner.");
                }
            }

            Directory.CreateDirectory(directory);
            return new RegistrationRef
            {
                Slug = slug,
                Directory = directory,
                GitHubUrl = gitHubUrl,
                IsLegacy = false,
            };
        }

        public void DeleteRegistration(string slug)
        {
            ArgUtil.NotNullOrEmpty(slug, nameof(slug));
            var registrationsRoot = Path.Combine(RootDirectory, Constants.MultiConfig.RegistrationsDirectory);
            var directory = Path.Combine(registrationsRoot, slug);
            if (Directory.Exists(directory))
            {
                Trace.Info($"Deleting registration directory: {directory}");
                IOUtil.DeleteDirectory(directory, CancellationToken.None);
            }

            if (Directory.Exists(registrationsRoot) && !Directory.EnumerateFileSystemEntries(registrationsRoot).Any())
            {
                Directory.Delete(registrationsRoot);
            }
        }

        public RegistrationRef MigrateLegacyToSlug()
        {
            var legacySettings = TryLoadSettings(RootDirectory);
            if (legacySettings == null)
            {
                throw new InvalidOperationException("No legacy runner configuration found to migrate.");
            }

            if (string.IsNullOrEmpty(legacySettings.GitHubUrl))
            {
                throw new InvalidOperationException("The existing runner configuration has no GitHub URL and cannot be migrated to the multi-repository layout.");
            }

            Trace.Info($"Migrating legacy configuration for '{legacySettings.GitHubUrl}' into the multi-repository layout.");
            var registration = CreateSlugDirectory(legacySettings.GitHubUrl);

            // Copy-verify-then-delete so a crash mid-migration never loses the
            // only copy of a credential.
            foreach (var fileName in RegistrationFileNames)
            {
                var source = Path.Combine(RootDirectory, fileName);
                if (!File.Exists(source))
                {
                    continue;
                }

                var destination = Path.Combine(registration.Directory, fileName);
                File.Copy(source, destination, overwrite: true);
                if (new FileInfo(destination).Length != new FileInfo(source).Length)
                {
                    throw new InvalidOperationException($"Failed to migrate '{source}' to '{destination}'.");
                }

                if (fileName == ".credentials_rsaparams" && !OperatingSystem.IsWindows())
                {
                    File.SetUnixFileMode(destination, UnixFileMode.UserRead | UnixFileMode.UserWrite);
                }
            }

            foreach (var fileName in RegistrationFileNames)
            {
                IOUtil.Delete(Path.Combine(RootDirectory, fileName), CancellationToken.None);
            }

            registration.GitHubUrl = legacySettings.GitHubUrl;
            return registration;
        }

        public static string NormalizeGitHubUrl(string gitHubUrl)
        {
            ArgUtil.NotNullOrEmpty(gitHubUrl, nameof(gitHubUrl));
            var uriBuilder = new UriBuilder(gitHubUrl.Trim());
            var path = uriBuilder.Path.TrimEnd('/');
            if (path.EndsWith(".git", StringComparison.OrdinalIgnoreCase))
            {
                path = path.Substring(0, path.Length - 4);
            }

            var port = uriBuilder.Uri.IsDefaultPort ? string.Empty : $":{uriBuilder.Port}";
            return $"{uriBuilder.Scheme.ToLowerInvariant()}://{uriBuilder.Host.ToLowerInvariant()}{port}{path.ToLowerInvariant()}";
        }

        public static string MakeSlug(string gitHubUrl)
        {
            var normalized = NormalizeGitHubUrl(gitHubUrl);
            var uri = new Uri(normalized);
            var name = uri.AbsolutePath.Trim('/');
            if (string.IsNullOrEmpty(name))
            {
                name = uri.Host;
            }

            return Regex.Replace(name.Replace('/', '-'), "[^0-9a-zA-Z._-]", "-");
        }

        private IEnumerable<string> EnumerateRegistrationDirectories()
        {
            var registrationsRoot = Path.Combine(RootDirectory, Constants.MultiConfig.RegistrationsDirectory);
            if (!Directory.Exists(registrationsRoot))
            {
                return Enumerable.Empty<string>();
            }

            return Directory.EnumerateDirectories(registrationsRoot)
                .Where(IsOccupied)
                .OrderBy(dir => dir, StringComparer.Ordinal);
        }

        private bool HasLegacyConfig()
        {
            return File.Exists(Path.Combine(RootDirectory, ".runner")) ||
                File.Exists(Path.Combine(RootDirectory, ".runner_migrated"));
        }

        private static bool IsOccupied(string directory)
        {
            return Directory.Exists(directory) &&
                (File.Exists(Path.Combine(directory, ".runner")) || File.Exists(Path.Combine(directory, ".runner_migrated")));
        }

        private RunnerSettings TryLoadSettings(string directory)
        {
            foreach (var fileName in new[] { ".runner", ".runner_migrated" })
            {
                var path = Path.Combine(directory, fileName);
                if (!File.Exists(path))
                {
                    continue;
                }

                try
                {
                    return IOUtil.LoadObject<RunnerSettings>(path);
                }
                catch (Exception ex)
                {
                    Trace.Warning($"Failed to parse '{path}': {ex.Message}");
                }
            }

            return null;
        }
    }
}
