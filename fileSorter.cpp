#include <iostream>
#include <filesystem>
#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <vector>
#include <mutex>
#include <queue>
#include <iomanip>
#include <openssl/md5.h>

namespace fs = std::filesystem;

// Mutex for synchronized access to shared resources (log file, file list)
std::mutex logMutex;
std::mutex queueMutex;
std::ofstream logFile("log.txt");

// Thread-safe logging function
void logOperation(const std::string& message) {
    std::lock_guard<std::mutex> guard(logMutex);
    logFile << message << std::endl;
}

// Function to calculate MD5 hash of a file
std::string calculateMD5(const fs::path& filePath) {
    std::ifstream file(filePath, std::ios::binary);
    MD5_CTX md5Context;
    MD5_Init(&md5Context);

    char buffer[1024];
    while (file.read(buffer, sizeof(buffer))) {
        MD5_Update(&md5Context, buffer, file.gcount());
    }
    if (file.gcount() > 0) {
        MD5_Update(&md5Context, buffer, file.gcount());
    }

    unsigned char hash[MD5_DIGEST_LENGTH];
    MD5_Final(hash, &md5Context);

    std::ostringstream oss;
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return oss.str();
}

// Function to move a file to the appropriate directory based on extension
void moveFile(const fs::path& src, const fs::path& destDir, std::unordered_map<std::string, std::string>& fileHashes) {
    try {
        // Check if destination directory exists, if not create it
        if (!fs::exists(destDir)) {
            fs::create_directory(destDir);
        }

        // Check file content with MD5 checksum
        std::string fileHash = calculateMD5(src);
        if (fileHashes.find(fileHash) != fileHashes.end()) {
            std::string newName = src.stem().string() + "_duplicate" + src.extension().string();
            fs::path duplicateDest = destDir / newName;
            fs::rename(src, duplicateDest);
            logOperation("Duplicate file found: " + src.string() + " (renamed to: " + duplicateDest.string() + ")");
        } else {
            // Move the file
            fs::path destPath = destDir / src.filename();
            fs::rename(src, destPath);
            logOperation("Moved: " + src.string() + " -> " + destPath.string());
            fileHashes[fileHash] = destPath.string();
        }
    } catch (const fs::filesystem_error& e) {
        logOperation("Error moving file: " + std::string(e.what()));
    }
}

// Function to load recovery state
std::unordered_set<std::string> loadRecovery(const std::string& recoveryFile) {
    std::ifstream file(recoveryFile);
    std::unordered_set<std::string> processedFiles;
    std::string line;
    while (std::getline(file, line)) {
        processedFiles.insert(line);
    }
    return processedFiles;
}

// Function to save sorted files to recovery file
void saveToRecovery(const std::string& recoveryFile, const std::string& filePath) {
    std::ofstream file(recoveryFile, std::ios::app);
    file << filePath << std::endl;
}

// Function to print progress bar
void printProgress(size_t current, size_t total) {
    const int barWidth = 70;
    float progress = (float)current / total;
    std::cout << "[";
    int pos = barWidth * progress;
    for (int i = 0; i < barWidth; ++i) {
        if (i < pos) std::cout << "=";
        else if (i == pos) std::cout << ">";
        else std::cout << " ";
    }
    std::cout << "] " << int(progress * 100.0) << " %\r";
    std::cout.flush();
}

// Thread worker function
void worker(std::queue<fs::path>& filesQueue, const fs::path& rootDir, std::unordered_set<std::string>& processedFiles,
            std::unordered_map<std::string, std::string>& fileHashes, std::mutex& queueMutex, size_t totalFiles) {
    size_t processedFilesCount = 0;
    while (true) {
        fs::path filePath;

        // Lock the queue and retrieve a file to process
        {
            std::lock_guard<std::mutex> guard(queueMutex);
            if (filesQueue.empty()) {
                break;
            }
            filePath = filesQueue.front();
            filesQueue.pop();
        }

        // Get the file extension and create a folder for it
        std::string extension = filePath.extension().string();
        if (extension.empty()) {
            extension = "no_extension";
        }

        fs::path destDir = rootDir / extension;

        // Check for duplicate processing of files
        if (processedFiles.find(filePath.string()) == processedFiles.end()) {
            moveFile(filePath, destDir, fileHashes);
            processedFiles.insert(filePath.string());
            saveToRecovery("recovery.txt", filePath.string());
        }

        // Update progress bar
        processedFilesCount++;
        printProgress(processedFilesCount, totalFiles);
    }
}

// Function to sort files using multithreading
void sortFiles(const fs::path& rootDir, int threadCount) {
    if (!fs::exists(rootDir) || !fs::is_directory(rootDir)) {
        std::cerr << "Invalid directory!" << std::endl;
        return;
    }

    // Load recovery state
    std::unordered_set<std::string> processedFiles = loadRecovery("recovery.txt");
    std::unordered_map<std::string, std::string> fileHashes;
    std::queue<fs::path> filesQueue;

    // Recursively collect all files
    for (const auto& entry : fs::recursive_directory_iterator(rootDir)) {
        if (fs::is_regular_file(entry.path()) && processedFiles.find(entry.path().string()) == processedFiles.end()) {
            filesQueue.push(entry.path());
        }
    }

    size_t totalFiles = filesQueue.size();
    if (totalFiles == 0) {
        std::cerr << "No files to process!" << std::endl;
        return;
    }

    // Create thread pool
    std::vector<std::thread> workers;
    for (int i = 0; i < threadCount; ++i) {
        workers.emplace_back(worker, std::ref(filesQueue), std::ref(rootDir), std::ref(processedFiles), std::ref(fileHashes), std::ref(queueMutex), totalFiles);
    }

    // Join all threads
    for (auto& worker : workers) {
        worker.join();
    }

    std::cout << "\nFile sorting completed successfully!" << std::endl;
}

int main() {
    std::string rootFolder;
    std::cout << "Enter the root folder to sort files: ";
    std::getline(std::cin, rootFolder);

    fs::path rootDir(rootFolder);

    // Open log file
    logFile.open("log.txt", std::ios::out | std::ios::app);
    if (!logFile.is_open()) {
        std::cerr << "Error opening log file." << std::endl;
        return 1;
    }

    int threadCount = std::thread::hardware_concurrency(); // Use the maximum available threads

    try {
        logOperation("Starting file sorting...");
        sortFiles(rootDir, threadCount);
        logOperation("File sorting completed successfully.");
    } catch (const std::exception& e) {
        logOperation("An error occurred: " + std::string(e.what()));
        std::cerr << "An error occurred: " << e.what() << std::endl;
    }

    logFile.close();
    return 0;
}
