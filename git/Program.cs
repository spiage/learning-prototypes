using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

// ========================
// ФАЙЛОВЫЕ КОМПОНЕНТЫ
// ========================

public class GitHead_File
{
    private readonly string _gitDir;
    public GitHead_File(string gitDir) => _gitDir = gitDir;
    public void Init() => File.WriteAllText(Path.Combine(_gitDir, "HEAD"), "ref: refs/heads/main\n");
}

public class GitConfig_File
{
    private readonly string _gitDir;
    public GitConfig_File(string gitDir) => _gitDir = gitDir;
    public void Init()
    {
        var content = new StringBuilder();
        content.AppendLine("[core]");
        content.AppendLine("\trepositoryformatversion = 0");
        content.AppendLine("\tfilemode = false");
        content.AppendLine("\tbare = false");
        content.AppendLine("\tlogallrefupdates = true");
        File.WriteAllText(Path.Combine(_gitDir, "config"), content.ToString());
    }
}

public class GitDescription_File
{
    private readonly string _gitDir;
    public GitDescription_File(string gitDir) => _gitDir = gitDir;
    public void Init() => File.WriteAllText(
        Path.Combine(_gitDir, "description"),
        "Unnamed repository; edit this file 'description' to name the repository.\n");
}

// ========================
// ДИРЕКТОРИИ
// ========================

public class GitObjects_Dir
{
    private readonly string _path;
    public GitObjects_Dir(string gitDir) => _path = Path.Combine(gitDir, "objects");
    public void Init()
    {
        Directory.CreateDirectory(_path);
        Directory.CreateDirectory(Path.Combine(_path, "info"));
        Directory.CreateDirectory(Path.Combine(_path, "pack"));
    }
}

public class GitInfo_Dir
{
    private readonly string _path;
    public GitInfo_Dir(string gitDir) => _path = Path.Combine(gitDir, "info");
    public void Init()
    {
        Directory.CreateDirectory(_path);
        File.WriteAllText(Path.Combine(_path, "exclude"), "");
    }
}

public class GitHooks_Dir
{
    private readonly string _path;
    public GitHooks_Dir(string gitDir) => _path = Path.Combine(gitDir, "hooks");
    public void Init() => Directory.CreateDirectory(_path);
}

public class GitRefsTags_Dir
{
    private readonly string _path;
    public GitRefsTags_Dir(string refsDir) => _path = Path.Combine(refsDir, "tags");
    public void Init() => Directory.CreateDirectory(_path);
}

public class GitRefsHeads_Dir
{
    private readonly string _path;
    public GitRefsHeads_Dir(string refsDir) => _path = Path.Combine(refsDir, "heads");
    public void Init() => Directory.CreateDirectory(_path);
    public void UpdateBranch(string branchName, string commitHash)
    {
        Directory.CreateDirectory(_path);
        File.WriteAllText(Path.Combine(_path, branchName), commitHash + "\n");
    }
}

public class GitRefs_Dir
{
    private readonly string _path;
    private readonly GitRefsHeads_Dir _heads;
    private readonly GitRefsTags_Dir _tags;

    public GitRefs_Dir(string gitDir)
    {
        _path = Path.Combine(gitDir, "refs");
        _heads = new GitRefsHeads_Dir(_path);
        _tags = new GitRefsTags_Dir(_path);
    }

    public void Init()
    {
        Directory.CreateDirectory(_path);
        _heads.Init();
        _tags.Init();
    }

    public void UpdateBranch(string branchName, string commitHash) =>
        _heads.UpdateBranch(branchName, commitHash);
}

public class GitLogs_Dir
{
    private readonly string _path;

    public GitLogs_Dir(string gitDir)
    {
        _path = Path.Combine(gitDir, "logs");
    }

    public void Init()
    {
        Directory.CreateDirectory(_path);
        Directory.CreateDirectory(Path.Combine(_path, "refs"));
        Directory.CreateDirectory(Path.Combine(_path, "refs", "heads"));
    }

    public void AppendToLog(string relativeLogPath, string oldHash, string newHash, string committer, string message)
    {
        string fullPath = Path.Combine(_path, relativeLogPath);
        
        // Исправляем ошибку: создаём директорию, только если она есть в пути
        string? logDir = Path.GetDirectoryName(fullPath);
        if (logDir != null)
        {
            Directory.CreateDirectory(logDir);
        }

        var now = DateTimeOffset.Now;
        string timestamp = now.ToUnixTimeSeconds().ToString();
        string timezone = now.Offset.ToString("hhmm");
        
        string logLine = $"{oldHash} {newHash} {committer} {timestamp} {timezone}\t{message}\n";
        
        File.AppendAllText(fullPath, logLine);
    }
}

// ========================
// BLOB
// ========================

public class GitBlob_Object
{
    private readonly string _objectsDir;
    private readonly byte[] _contentBytes;

    public GitBlob_Object(string gitDir, string filePath)
    {
        _objectsDir = Path.Combine(gitDir, "objects");
        _contentBytes = File.ReadAllBytes(filePath);
    }

    private byte[] GetFullData()
    {
        var header = Encoding.UTF8.GetBytes($"blob {_contentBytes.Length}\0");
        return header.Concat(_contentBytes).ToArray();
    }

    public string Hash
    {
        get
        {
            using var sha1 = SHA1.Create();
            return BitConverter.ToString(sha1.ComputeHash(GetFullData())).Replace("-", "").ToLower();
        }
    }

    public void Save()
    {
        string hash = Hash;
        string dir = Path.Combine(_objectsDir, hash.Substring(0, 2));
        string file = Path.Combine(dir, hash.Substring(2));
        Directory.CreateDirectory(dir);

        var compressed = Compress(GetFullData());
        File.WriteAllBytes(file, compressed);
    }

    private static byte[] Compress(byte[] data)
    {
        using var output = new MemoryStream();
        using (var deflate = new DeflateStream(output, CompressionLevel.Optimal))
        {
            deflate.Write(data, 0, data.Length);
        }
        return output.ToArray();
    }
}

// ========================
// INDEX
// ========================

public class GitIndex_File
{
    private readonly string _indexPath;
    private readonly string _workingDir;
    private readonly List<(string Path, string Hash, FileInfo Info)> _entries = new();

    public GitIndex_File(string gitDir, string workingDir = ".")
    {
        _indexPath = Path.Combine(gitDir, "index");
        _workingDir = workingDir;
    }

    public void AddEntry(string relativePath, string blobHash, FileInfo info) =>
        _entries.Add((relativePath, blobHash, info));

    public List<(string Path, string Hash, FileInfo Info)> GetEntries() => new(_entries);

    public void WriteIndex()
    {
        if (File.Exists(_indexPath))
            File.Delete(_indexPath);

        using var stream = new FileStream(_indexPath, FileMode.Create, FileAccess.Write);
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: false);

        writer.Write(0x44495243); // "DIRC" (little-endian)
        writer.Write((uint)2);
        writer.Write((uint)_entries.Count);

        foreach (var (relativePath, blobHash, info) in _entries)
        {
            var now = DateTimeOffset.Now;
            uint sec = (uint)now.ToUnixTimeSeconds();
            long ticksInSec = now.Ticks % TimeSpan.TicksPerSecond;
            uint nsec = (uint)(ticksInSec * 100); // приблизительно

            writer.Write(sec); writer.Write(nsec);
            writer.Write(sec); writer.Write(nsec);
            writer.Write((uint)0); writer.Write((uint)0);
            writer.Write((uint)0100644); writer.Write((uint)0); writer.Write((uint)0);
            writer.Write((long)info.Length);

            byte[] hashBytes = Convert.FromHexString(blobHash);
            writer.Write(hashBytes);

            byte nameLen = (byte)relativePath.Length;
            writer.Write(nameLen);
            writer.Write(Encoding.UTF8.GetBytes(relativePath));

            int entrySize = 62 + nameLen;
            int pad = (8 - (entrySize % 8)) % 8;
            for (int i = 0; i < pad; i++) writer.Write((byte)0);
        }
    }
}

// ========================
// TREE
// ========================

public class GitTree_Object
{
    private readonly string _objectsDir;
    private readonly List<TreeEntry> _entries = new();

    public GitTree_Object(string gitDir) =>
        _objectsDir = Path.Combine(gitDir, "objects");

    public class TreeEntry
    {
        public string Name { get; set; }
        public string Hash { get; set; }
        public string Type { get; set; }
    }

    public void AddEntry(string name, string hash, string type) =>
        _entries.Add(new TreeEntry { Name = name, Hash = hash, Type = type });

    private byte[] BuildContent()
    {
        using var ms = new MemoryStream();
        foreach (var e in _entries.OrderBy(x => x.Name))
        {
            string modeStr = e.Type == "tree" ? "040000" : "100644";
            byte[] modeBytes = Encoding.UTF8.GetBytes(modeStr);
            ms.Write(modeBytes, 0, modeBytes.Length);
            ms.WriteByte((byte)' '); // Пробел обязателен
            byte[] nameBytes = Encoding.UTF8.GetBytes(e.Name);
            ms.Write(nameBytes, 0, nameBytes.Length);
            ms.WriteByte(0); // null terminator
            ms.Write(Convert.FromHexString(e.Hash), 0, 20);
        }
        return ms.ToArray();
    }

    private byte[] GetFullData()
    {
        var content = BuildContent();
        var header = Encoding.UTF8.GetBytes($"tree {content.Length}\0");
        return header.Concat(content).ToArray();
    }

    public string Hash
    {
        get
        {
            using var sha1 = SHA1.Create();
            return BitConverter.ToString(sha1.ComputeHash(GetFullData())).Replace("-", "").ToLower();
        }
    }

    public void Save()
    {
        string hash = Hash;
        string dir = Path.Combine(_objectsDir, hash.Substring(0, 2));
        string file = Path.Combine(dir, hash.Substring(2));
        Directory.CreateDirectory(dir);

        var compressed = Compress(GetFullData());
        File.WriteAllBytes(file, compressed);
    }

    private static byte[] Compress(byte[] data)
    {
        using var output = new MemoryStream();
        using (var deflate = new DeflateStream(output, CompressionLevel.Optimal))
        {
            deflate.Write(data, 0, data.Length);
        }
        return output.ToArray();
    }
}

// ========================
// COMMIT
// ========================

public class GitCommit_Object
{
    private readonly string _objectsDir;
    private readonly byte[] _fullData;

    public GitCommit_Object(string gitDir, string treeHash, string? parentHash, string author, string message)
    {
        _objectsDir = Path.Combine(gitDir, "objects");
        var sb = new StringBuilder();
        sb.AppendLine($"tree {treeHash}");
        if (!string.IsNullOrEmpty(parentHash))
            sb.AppendLine($"parent {parentHash}");
        string timestamp = GetGitTimestamp();
        sb.AppendLine($"author {author} {timestamp}");
        sb.AppendLine($"committer {author} {timestamp}");
        sb.AppendLine();
        sb.AppendLine(message);
        var content = Encoding.UTF8.GetBytes(sb.ToString());
        var header = Encoding.UTF8.GetBytes($"commit {content.Length}\0");
        _fullData = header.Concat(content).ToArray();
    }

    private static string GetGitTimestamp()
    {
        var now = DateTimeOffset.Now;
        return $"{now.ToUnixTimeSeconds()} {now.Offset:hhmm}";
    }

    public string Hash
    {
        get
        {
            using var sha1 = SHA1.Create();
            return BitConverter.ToString(sha1.ComputeHash(_fullData)).Replace("-", "").ToLower();
        }
    }

    public void Save()
    {
        string hash = Hash;
        string dir = Path.Combine(_objectsDir, hash.Substring(0, 2));
        string file = Path.Combine(dir, hash.Substring(2));
        Directory.CreateDirectory(dir);

        var compressed = Compress(_fullData);
        File.WriteAllBytes(file, compressed);
    }

    private static byte[] Compress(byte[] data)
    {
        using var output = new MemoryStream();
        using (var deflate = new DeflateStream(output, CompressionLevel.Optimal))
        {
            deflate.Write(data, 0, data.Length);
        }
        return output.ToArray();
    }
}

// ========================
// ОСНОВНОЙ КЛАСС
// ========================

public class GitStore
{
    private readonly string _gitDir;
    private readonly string _workingDir;
    private readonly GitHead_File _head;
    private readonly GitConfig_File _config;
    private readonly GitDescription_File _description;
    private readonly GitObjects_Dir _objects;
    private readonly GitRefs_Dir _refs;
    private readonly GitInfo_Dir _info;
    private readonly GitHooks_Dir _hooks;
    private readonly GitIndex_File _index;
    private readonly GitLogs_Dir _logs;

    public GitStore(string workingDir = ".")
    {
        _workingDir = workingDir;
        _gitDir = Path.Combine(workingDir, ".git");
        _head = new GitHead_File(_gitDir);
        _config = new GitConfig_File(_gitDir);
        _description = new GitDescription_File(_gitDir);
        _objects = new GitObjects_Dir(_gitDir);
        _refs = new GitRefs_Dir(_gitDir);
        _info = new GitInfo_Dir(_gitDir);
        _hooks = new GitHooks_Dir(_gitDir);
        _index = new GitIndex_File(_gitDir, _workingDir);
        _logs = new GitLogs_Dir(_gitDir);
    }

    public void Init()
    {
        if (Directory.Exists(_gitDir))
        {
            Console.WriteLine($"Reinitialized existing Git repository in {Path.GetFullPath(_gitDir)}");
            return;
        }

        Directory.CreateDirectory(_gitDir);
        _head.Init();
        _config.Init();
        _description.Init();
        _objects.Init();
        _refs.Init();
        _info.Init();
        _hooks.Init();
        Console.WriteLine($"Initialized empty Git repository in {Path.GetFullPath(_gitDir)}");
    }

    public void AddFile(string relativePath)
    {
        string fullPath = Path.Combine(_workingDir, relativePath);
        if (!File.Exists(fullPath))
            throw new FileNotFoundException($"File not found: {fullPath}");

        var blob = new GitBlob_Object(_gitDir, fullPath);
        blob.Save();
        var info = new FileInfo(fullPath);
        _index.AddEntry(relativePath, blob.Hash, info);
    }

    public void FlushIndex() => _index.WriteIndex();

    private string GetCommitterFromConfig()
    {
        string configPath = Path.Combine(_gitDir, "config");
        if (!File.Exists(configPath))
            throw new InvalidOperationException("Missing .git/config");

        var lines = File.ReadAllLines(configPath);
        string? name = null, email = null;
        bool inUserSection = false;

        foreach (var line in lines)
        {
            var trimmed = line.Trim();
            if (trimmed.StartsWith("[user]"))
            {
                inUserSection = true;
                continue;
            }
            if (trimmed.StartsWith("[") && trimmed.EndsWith("]"))
            {
                inUserSection = false;
                continue;
            }
            if (inUserSection)
            {
                if (trimmed.StartsWith("name = "))
                    name = trimmed["name = ".Length..].Trim(' ', '"', '\'');
                else if (trimmed.StartsWith("email = "))
                    email = trimmed["email = ".Length..].Trim(' ', '"', '\'');
            }
        }

        if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(email))
        {
            throw new InvalidOperationException(
                "*** Please tell me who you are.\n\n" +
                "Run\n\n" +
                "  git config --global user.email \"you@example.com\"\n" +
                "  git config --global user.name \"Your Name\"\n\n" +
                "to set your account's default identity.");
        }

        return $"{name} <{email}>";
    }

    public string Commit(string message)
    {
        var entries = _index.GetEntries();
        if (entries.Count == 0)
            throw new InvalidOperationException("Nothing to commit.");

        var tree = BuildTreeFromIndex(entries.Select(e => (e.Path, e.Hash)).ToList());
        tree.Save();

        var author = GetCommitterFromConfig();
        
        // Определяем старый хеш (для первого коммита он будет нулевым)
        string oldHeadHash = "0000000000000000000000000000000000000000";
        string headFilePath = Path.Combine(_gitDir, "refs", "heads", "main");
        if (File.Exists(headFilePath))
        {
            oldHeadHash = File.ReadAllText(headFilePath).Trim();
        }

        var commit = new GitCommit_Object(_gitDir, tree.Hash, oldHeadHash, author, message);
        commit.Save();

        _refs.UpdateBranch("main", commit.Hash);

        // --- ДОБАВЛЯЕМ ЗАПИСЬ В ЛОГИ ---
        // Создаём logs при первом коммите, если их еще нет
        if (!Directory.Exists(_logs._path))
        {
            _logs.Init();
        }

        // Определяем сообщение для лога HEAD
        string headMessage = File.Exists(headFilePath) ? $"commit: {message}" : $"commit (initial): {message}";

        _logs.AppendToLog("HEAD", oldHeadHash, commit.Hash, author, headMessage);
        _logs.AppendToLog("refs/heads/main", oldHeadHash, commit.Hash, author, message);
        // -------------------------------------------------

        Console.WriteLine($"[{commit.Hash[..7]}] {message}");
        return commit.Hash;
    }

    // === ИСПРАВЛЕННОЕ РЕКУРСИВНОЕ ПОСТРОЕНИЕ TREE ===
    // Этот метод сначала строит в памяти полное дерево директорий, чтобы избежать дублирования,
    // а затем рекурсивно создает объекты Git снизу-вверх (от самых глубоких к корню).

    private GitTree_Object BuildTreeFromIndex(List<(string Path, string Hash)> entries)
    {
        // 1. Строим в памяти структуру всех директорий и их содержимого
        var dirContents = new Dictionary<string, List<(string Name, string Hash, string Type)>>();
        foreach (var (path, hash) in entries)
        {
            var parts = path.Split('/');
            for (int i = 0; i < parts.Length; i++)
            {
                var currentDirPath = string.Join("/", parts.Take(i));
                var itemName = parts[i];

                if (!dirContents.ContainsKey(currentDirPath))
                {
                    dirContents[currentDirPath] = new List<(string, string, string)>();
                }

                if (i == parts.Length - 1) // Это файл
                {
                    dirContents[currentDirPath].Add((itemName, hash, "blob"));
                }
                else // Это поддиректория
                {
                    if (!dirContents[currentDirPath].Any(e => e.Name == itemName && e.Type == "tree"))
                    {
                        dirContents[currentDirPath].Add((itemName, "", "tree"));
                    }
                }
            }
        }

        // 2. Рекурсивно создаем Git-объекты деревьев, начиная с самых глубоких
        var createdTrees = new Dictionary<string, string>();
        CreateTreeObjectsRecursive("", dirContents, createdTrees);

        // 3. Возвращаем корневое дерево
        var rootTree = new GitTree_Object(_gitDir);
        foreach (var entry in dirContents[""])
        {
            var entryHash = entry.Type == "tree" ? createdTrees[entry.Name] : entry.Hash;
            rootTree.AddEntry(entry.Name, entryHash, entry.Type);
        }
        return rootTree;
    }

    private void CreateTreeObjectsRecursive(string dirPath, Dictionary<string, List<(string Name, string Hash, string Type)>> dirContents, Dictionary<string, string> createdTrees)
    {
        // Сначала рекурсивно обрабатываем все поддиректории
        foreach (var (name, _, type) in dirContents.GetValueOrDefault(dirPath, new List<(string, string, string)>()))
        {
            if (type == "tree")
            {
                var subDirPath = string.IsNullOrEmpty(dirPath) ? name : $"{dirPath}/{name}";
                CreateTreeObjectsRecursive(subDirPath, dirContents, createdTrees);
            }
        }

        // Теперь, когда все поддиректории созданы и у них есть хеши, создаем текущую
        var tree = new GitTree_Object(_gitDir);
        foreach (var (name, hash, type) in dirContents.GetValueOrDefault(dirPath, new List<(string, string, string)>()))
        {
            if (type == "tree")
            {
                var subDirPath = string.IsNullOrEmpty(dirPath) ? name : $"{dirPath}/{name}";
                var subTreeHash = createdTrees[subDirPath];
                tree.AddEntry(name, subTreeHash, "tree");
            }
            else
            {
                tree.AddEntry(name, hash, "blob");
            }
        }
        tree.Save();
        createdTrees[dirPath] = tree.Hash;
    }
}

// ========================
// ТОЧКА ВХОДА
// ========================

class Program
{
    static void Main()
    {
        try
        {
            // Очистка и создание тестовых файлов
            if (Directory.Exists(".git")) Directory.Delete(".git", true);
            if (File.Exists("first.txt")) File.Delete("first.txt");
            if (Directory.Exists("second")) Directory.Delete("second", true);
            if (Directory.Exists("a")) Directory.Delete("a", true);

            File.WriteAllText("first.txt", "line1\n");
            Directory.CreateDirectory("second");
            File.WriteAllText("second/third.txt", "line2\n");
            Directory.CreateDirectory("a/b");
            File.WriteAllText("a/b/c.txt", "deep\n");

            var store = new GitStore();
            store.Init();

            // Добавляем user.name/email для коммита
            var configPath = ".git/config";
            var configLines = File.ReadAllLines(configPath).ToList();
            configLines.Insert(5, "\tname = Test User");
            configLines.Insert(6, "\temail = test@example.com");
            File.WriteAllLines(configPath, configLines);

            store.AddFile("first.txt");
            store.AddFile("second/third.txt");
            store.AddFile("a/b/c.txt");
            store.FlushIndex();

            var commitHash = store.Commit("Initial commit with deep nesting");
            Console.WriteLine($"Commit hash: {commitHash}");
            Console.WriteLine("Success! Try: git log --oneline");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }
}
