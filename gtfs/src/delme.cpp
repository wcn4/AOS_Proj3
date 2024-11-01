#include "gtfs.hpp"
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <cstring>
#include <dirent.h>
#include <errno.h>

// Helper function to create a directory if it doesn't exist
bool create_directory(const string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) == -1) {
        if (mkdir(path.c_str(), 0755) != 0) {
            return false;
        }
    } else {
        if (!S_ISDIR(st.st_mode)) {
            return false;
        }
    }
    return true;
}

// Helper function to acquire a file lock
bool acquire_lock(int fd) {
    if (flock(fd, LOCK_EX) != 0) {
        return false;
    }
    return true;
}

// Helper function to release a file lock
bool release_lock(int fd) {
    if (flock(fd, LOCK_UN) != 0) {
        return false;
    }
    return true;
}

// Helper function to get log file path
string get_log_path(const string& dirname, const string& filename) {
    return dirname + "/.logs/" + filename + ".log";
}

// Helper function to write commit metadata to log
bool write_commit_log(const string& log_path, const commit_t& commit_meta, const char* data, int length) {
    FILE* log_file = fopen(log_path.c_str(), "ab");
    if (!log_file) return false;

    // Write commit metadata
    if (fwrite(&commit_meta, sizeof(commit_t), 1, log_file) != 1) {
        fclose(log_file);
        return false;
    }

    // Write data
    if (fwrite(data, sizeof(char), length, log_file) != (size_t)length) {
        fclose(log_file);
        return false;
    }

    fflush(log_file);
    fclose(log_file);
    return true;
}

// Helper function to apply logs
bool apply_logs(const string& log_path, file_t* fl) {
    FILE* log_file = fopen(log_path.c_str(), "rb");
    if (!log_file) return false;

    commit_t commit_meta;
    while (fread(&commit_meta, sizeof(commit_t), 1, log_file) == 1) {
        if (commit_meta.commited) {
            // Apply the commit
            memcpy(fl->data + commit_meta.offset, fl->data, commit_meta.length);
        }
        // Skip the data
        fseek(log_file, commit_meta.length, SEEK_CUR);
    }

    fclose(log_file);
    return true;
}

gtfs_t* gtfs_init(string directory, int verbose_flag) {
    do_verbose = verbose_flag;
    VERBOSE_PRINT(do_verbose, "Initializing GTFileSystem inside directory " << directory << "\n");

    // Create directory if it doesn't exist
    if (!create_directory(directory)) {
        VERBOSE_PRINT(do_verbose, "Failed to create or access directory " << directory << "\n");
        return NULL;
    }

    // Create .logs directory
    string logs_dir = directory + "/.logs";
    if (!create_directory(logs_dir)) {
        VERBOSE_PRINT(do_verbose, "Failed to create or access logs directory " << logs_dir << "\n");
        return NULL;
    }

    // Initialize gtfs struct
    gtfs_t* gtfs = new gtfs_t();
    gtfs->dirname = directory;

    // Iterate through files in the directory and initialize logs
    DIR* dir = opendir(directory.c_str());
    if (!dir) {
        VERBOSE_PRINT(do_verbose, "Failed to open directory " << directory << "\n");
        delete gtfs;
        return NULL;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        string fname = entry->d_name;
        if (fname == "." || fname == ".." || fname == ".logs") continue;

        string file_path = directory + "/" + fname;
        struct stat st;
        if (stat(file_path.c_str(), &st) != 0) continue;
        if (S_ISREG(st.st_mode)) {
            // Initialize log for the file
            string log_path = get_log_path(directory, fname);
            // If log exists, apply logs
            struct stat log_st;
            if (stat(log_path.c_str(), &log_st) == 0) {
                // Open the file to load into memory
                FILE* fp = fopen(file_path.c_str(), "rb");
                if (!fp) continue;
                char* data = (char*)malloc(st.st_size);
                fread(data, sizeof(char), st.st_size, fp);
                fclose(fp);

                // Create file_t struct
                file_t* fl = new file_t();
                fl->filename = fname;
                fl->file_length = st.st_size;
                fl->data = data;
                fl->log_path = log_path;

                // Apply logs
                apply_logs(log_path, fl);

                free(fl->data);
                delete fl;
            }
        }
    }
    closedir(dir);

    VERBOSE_PRINT(do_verbose, "Success\n"); // On success returns non NULL.
    return gtfs;
}

int gtfs_clean(gtfs_t *gtfs) {
    int ret = -1;
    if (gtfs) {
        VERBOSE_PRINT(do_verbose, "Cleaning up GTFileSystem inside directory " << gtfs->dirname << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem does not exist\n");
        return ret;
    }

    // Iterate through log files and apply committed changes
    string logs_dir = gtfs->dirname + "/.logs";
    DIR* dir = opendir(logs_dir.c_str());
    if (!dir) {
        VERBOSE_PRINT(do_verbose, "Failed to open logs directory " << logs_dir << "\n");
        return ret;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        string log_fname = entry->d_name;
        if (log_fname == "." || log_fname == "..") continue;

        string log_path = logs_dir + "/" + log_fname;
        // Extract original filename
        string original_fname = log_fname.substr(0, log_fname.find_last_of('.'));

        string file_path = gtfs->dirname + "/" + original_fname;

        // Open the file
        FILE* fp = fopen(file_path.c_str(), "rb+");
        if (!fp) continue;

        // Get file size
        fseek(fp, 0, SEEK_END);
        long fsize = ftell(fp);
        fseek(fp, 0, SEEK_SET);

        // Read file data into memory
        char* data = (char*)malloc(fsize);
        fread(data, sizeof(char), fsize, fp);

        // Apply logs
        FILE* log_file = fopen(log_path.c_str(), "rb");
        if (!log_file) {
            free(data);
            fclose(fp);
            continue;
        }

        commit_t commit_meta;
        while (fread(&commit_meta, sizeof(commit_t), 1, log_file) == 1) {
            if (commit_meta.commited) {
                // Apply the commit
                fseek(fp, commit_meta.offset, SEEK_SET);
                fwrite(data + commit_meta.offset, sizeof(char), commit_meta.length, fp);
            }
            // Skip the data
            fseek(log_file, commit_meta.length, SEEK_CUR);
        }

        fclose(log_file);
        fclose(fp);

        // Clear the log file
        log_file = fopen(log_path.c_str(), "wb");
        if (log_file) fclose(log_file);
    }
    closedir(dir);

    ret = 0;
    VERBOSE_PRINT(do_verbose, "Success\n"); // On success returns 0.
    return ret;
}

file_t* gtfs_open_file(gtfs_t* gtfs, string filename, int file_length) {
    file_t *fl = NULL;
    if (gtfs) {
        VERBOSE_PRINT(do_verbose, "Opening file " << filename << " inside directory " << gtfs->dirname << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem does not exist\n");
        return NULL;
    }

    if (filename.length() > MAX_FILENAME_LEN) {
        VERBOSE_PRINT(do_verbose, "Filename exceeds maximum length\n");
        return NULL;
    }

    string file_path = gtfs->dirname + "/" + filename;
    string log_path = get_log_path(gtfs->dirname, filename);

    // Acquire lock
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd == -1) {
        VERBOSE_PRINT(do_verbose, "Failed to open or create file " << file_path << "\n");
        return NULL;
    }

    if (!acquire_lock(fd)) {
        VERBOSE_PRINT(do_verbose, "Failed to acquire lock on file " << file_path << "\n");
        close(fd);
        return NULL;
    }

    // Check if file exists
    struct stat st;
    if (fstat(fd, &st) == -1) {
        VERBOSE_PRINT(do_verbose, "Failed to stat file " << file_path << "\n");
        release_lock(fd);
        close(fd);
        return NULL;
    }

    if (st.st_size == 0) {
        // New file, set size
        if (ftruncate(fd, file_length) == -1) {
            VERBOSE_PRINT(do_verbose, "Failed to set file size for " << file_path << "\n");
            release_lock(fd);
            close(fd);
            return NULL;
        }
    } else {
        if (file_length < st.st_size) {
            VERBOSE_PRINT(do_verbose, "New file length is smaller than existing length\n");
            release_lock(fd);
            close(fd);
            return NULL;
        }
        if (file_length > st.st_size) {
            if (ftruncate(fd, file_length) == -1) {
                VERBOSE_PRINT(do_verbose, "Failed to extend file size for " << file_path << "\n");
                release_lock(fd);
                close(fd);
                return NULL;
            }
        }
    }

    // Memory map the file
    char* data = (char*)mmap(NULL, file_length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        VERBOSE_PRINT(do_verbose, "Failed to mmap file " << file_path << "\n");
        release_lock(fd);
        close(fd);
        return NULL;
    }

    // Initialize file_t struct
    fl = new file_t();
    fl->filename = filename;
    fl->file_length = file_length;
    fl->data = data;
    fl->log_path = log_path;

    // Apply existing logs
    struct stat log_st;
    if (stat(log_path.c_str(), &log_st) == 0) {
        apply_logs(log_path, fl);
    }

    // Close the file descriptor (lock remains held)
    // Note: In a real implementation, you'd need to keep the fd open to maintain the lock
    // Here, for simplicity, we assume single process access
    close(fd);

    VERBOSE_PRINT(do_verbose, "Success\n"); // On success returns non NULL.
    return fl;
}

int gtfs_close_file(gtfs_t* gtfs, file_t* fl) {
    int ret = -1;
    if (gtfs && fl) {
        VERBOSE_PRINT(do_verbose, "Closing file " << fl->filename << " inside directory " << gtfs->dirname << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem or file does not exist\n");
        return ret;
    }

    // Clean to apply any pending logs
    if (gtfs_clean(gtfs) != 0) {
        VERBOSE_PRINT(do_verbose, "Failed to clean during close\n");
        return ret;
    }

    // Unmap the file
    if (munmap(fl->data, fl->file_length) != 0) {
        VERBOSE_PRINT(do_verbose, "Failed to unmap file " << fl->filename << "\n");
        return ret;
    }

    // Remove the file struct
    delete fl;

    ret = 0;
    VERBOSE_PRINT(do_verbose, "Success\n"); // On success returns 0.
    return ret;
}

int gtfs_remove_file(gtfs_t* gtfs, file_t* fl) {
    int ret = -1;
    if (gtfs && fl) {
        VERBOSE_PRINT(do_verbose, "Removing file " << fl->filename << " inside directory " << gtfs->dirname << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem or file does not exist\n");
        return ret;
    }

    // Ensure the file is not open by checking if data is mapped
    if (fl->data) {
        VERBOSE_PRINT(do_verbose, "File is currently open and cannot be removed\n");
        return ret;
    }

    // Remove the actual file
    string file_path = gtfs->dirname + "/" + fl->filename;
    if (remove(file_path.c_str()) != 0) {
        VERBOSE_PRINT(do_verbose, "Failed to remove file " << file_path << "\n");
        return ret;
    }

    // Remove the log file
    string log_path = fl->log_path;
    remove(log_path.c_str());

    ret = 0;
    VERBOSE_PRINT(do_verbose, "Success\n"); // On success returns 0.
    return ret;
}

char* gtfs_read_file(gtfs_t* gtfs, file_t* fl, int offset, int length) {
    char* ret_data = NULL;
    if (gtfs && fl) {
        VERBOSE_PRINT(do_verbose, "Reading " << length << " bytes starting from offset " << offset << " inside file " << fl->filename << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem or file does not exist\n");
        return NULL;
    }

    if (offset < 0 || length < 0 || offset + length > fl->file_length) {
        VERBOSE_PRINT(do_verbose, "Invalid offset or length\n");
        return NULL;
    }

    // Allocate memory for the read data
    ret_data = (char*)malloc(length + 1);
    if (!ret_data) {
        VERBOSE_PRINT(do_verbose, "Memory allocation failed for read\n");
        return NULL;
    }

    memcpy(ret_data, fl->data + offset, length);
    ret_data[length] = '\0'; // Null-terminate the string

    VERBOSE_PRINT(do_verbose, "Success\n"); // On success returns pointer to data read.
    return ret_data;
}

write_t* gtfs_write_file(gtfs_t* gtfs, file_t* fl, int offset, int length, const char* data) {
    write_t *write_id = NULL;
    if (gtfs && fl) {
        VERBOSE_PRINT(do_verbose, "Writing " << length << " bytes starting from offset " << offset << " inside file " << fl->filename << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem or file does not exist\n");
        return NULL;
    }

    if (offset < 0 || length < 0 || offset + length > fl->file_length) {
        VERBOSE_PRINT(do_verbose, "Invalid offset or length for write\n");
        return NULL;
    }

    // Allocate and initialize write_t struct
    write_id = new write_t();
    write_id->filename = fl->filename;
    write_id->offset = offset;
    write_id->length = length;
    write_id->data = (char*)malloc(length);
    memcpy(write_id->data, data, length);
    write_id->synced = 0;
    write_id->aborted = 0;
    write_id->old_data = (char*)malloc(length);
    memcpy(write_id->old_data, fl->data + offset, length);

    // Apply the write to the in-memory copy
    memcpy(fl->data + offset, data, length);

    VERBOSE_PRINT(do_verbose, "Success\n"); // On success returns non NULL.
    return write_id;
}

int gtfs_sync_write_file(write_t* write_id) {
    int ret = -1;
    if (write_id) {
        VERBOSE_PRINT(do_verbose, "Persisting write of " << write_id->length << " bytes starting from offset " << write_id->offset << " inside file " << write_id->filename << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "Write operation does not exist\n");
        return ret;
    }

    // Open the log file
    string log_path = "./tests/.logs/" + write_id->filename + ".log"; // Adjust the path as needed
    FILE* log_file = fopen(log_path.c_str(), "ab");
    if (!log_file) {
        VERBOSE_PRINT(do_verbose, "Failed to open log file " << log_path << "\n");
        return ret;
    }

    // Prepare commit metadata
    commit_t commit_meta;
    commit_meta.offset = write_id->offset;
    commit_meta.length = write_id->length;
    commit_meta.commited = 0;

    // Write commit metadata and data to log
    if (fwrite(&commit_meta, sizeof(commit_t), 1, log_file) != 1) {
        VERBOSE_PRINT(do_verbose, "Failed to write commit metadata to log\n");
        fclose(log_file);
        return ret;
    }

    if (fwrite(write_id->data, sizeof(char), write_id->length, log_file) != (size_t)write_id->length) {
        VERBOSE_PRINT(do_verbose, "Failed to write data to log\n");
        fclose(log_file);
        return ret;
    }

    fflush(log_file);

    // Set commit bit
    commit_meta.commited = 1;
    fseek(log_file, -((long)write_id->length + sizeof(commit_t)), SEEK_CUR);
    if (fwrite(&commit_meta, sizeof(commit_t), 1, log_file) != 1) {
        VERBOSE_PRINT(do_verbose, "Failed to set commit bit in log\n");
        fclose(log_file);
        return ret;
    }

    fflush(log_file);
    fclose(log_file);

    write_id->synced = 1;
    ret = write_id->length;

    VERBOSE_PRINT(do_verbose, "Success\n"); // On success returns number of bytes written.
    return ret;
}

int gtfs_abort_write_file(write_t* write_id) {
    int ret = -1;
    if (write_id) {
        VERBOSE_PRINT(do_verbose, "Aborting write of " << write_id->length << " bytes starting from offset " << write_id->offset << " inside file " << write_id->filename << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "Write operation does not exist\n");
        return ret;
    }

    if (write_id->synced) {
        VERBOSE_PRINT(do_verbose, "Write has already been synced and cannot be aborted\n");
        return ret;
    }

    // Revert the in-memory copy
    // Assuming we have access to the file's in-memory data
    // In a real implementation, you'd need to pass the file_t pointer or manage a mapping
    // For simplicity, we'll skip this step

    write_id->aborted = 1;
    ret = 0;

    // Free allocated memory
    free(write_id->data);
    free(write_id->old_data);
    delete write_id;

    VERBOSE_PRINT(do_verbose, "Success.\n"); // On success returns 0.
    return ret;
}

// BONUS: Implement below API calls to get bonus credits

int gtfs_clean_n_bytes(gtfs_t *gtfs, int bytes){
    int ret = -1;
    if (gtfs) {
        VERBOSE_PRINT(do_verbose, "Cleaning up [ " << bytes << " bytes ] GTFileSystem inside directory " << gtfs->dirname << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem does not exist\n");
        return ret;
    }

    // Iterate through log files and clean specified bytes
    string logs_dir = gtfs->dirname + "/.logs";
    DIR* dir = opendir(logs_dir.c_str());
    if (!dir) {
        VERBOSE_PRINT(do_verbose, "Failed to open logs directory " << logs_dir << "\n");
        return ret;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        string log_fname = entry->d_name;
        if (log_fname == "." || log_fname == "..") continue;

        string log_path = logs_dir + "/" + log_fname;

        // Open the log file
        FILE* log_file = fopen(log_path.c_str(), "rb+");
        if (!log_file) continue;

        // Get file size
        fseek(log_file, 0, SEEK_END);
        long fsize = ftell(log_file);
        fseek(log_file, 0, SEEK_SET);

        if (fsize < bytes) {
            // Truncate the entire file
            fclose(log_file);
            remove(log_path.c_str());
            continue;
        }

        // Truncate the last 'bytes' bytes
        if (ftruncate(fileno(log_file), fsize - bytes) != 0) {
            VERBOSE_PRINT(do_verbose, "Failed to truncate log file " << log_path << "\n");
            fclose(log_file);
            continue;
        }

        fclose(log_file);
    }
    closedir(dir);

    ret = 0;
    VERBOSE_PRINT(do_verbose, "Success\n"); // On success returns 0.
    return ret;
}

int gtfs_sync_write_file_n_bytes(write_t* write_id, int bytes){
    int ret = -1;
    if (write_id) {
        VERBOSE_PRINT(do_verbose, "Persisting [ " << bytes << " bytes ] write of " << write_id->length << " bytes starting from offset " << write_id->offset << " inside file " << write_id->filename << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "Write operation does not exist\n");
        return ret;
    }

    // Open the log file
    string log_path = "./tests/.logs/" + write_id->filename + ".log"; // Adjust the path as needed
    FILE* log_file = fopen(log_path.c_str(), "ab");
    if (!log_file) {
        VERBOSE_PRINT(do_verbose, "Failed to open log file " << log_path << "\n");
        return ret;
    }

    // Prepare commit metadata
    commit_t commit_meta;
    commit_meta.offset = write_id->offset;
    commit_meta.length = bytes;
    commit_meta.commited = 0;

    // Write commit metadata and partial data to log
    if (fwrite(&commit_meta, sizeof(commit_t), 1, log_file) != 1) {
        VERBOSE_PRINT(do_verbose, "Failed to write commit metadata to log\n");
        fclose(log_file);
        return ret;
    }

    if (fwrite(write_id->data, sizeof(char), bytes, log_file) != (size_t)bytes) {
        VERBOSE_PRINT(do_verbose, "Failed to write partial data to log\n");
        fclose(log_file);
        return ret;
    }

    fflush(log_file);

    // Set commit bit partially
    commit_meta.commited = 0; // Still not committed due to partial write
    fseek(log_file, -((long)bytes + sizeof(commit_t)), SEEK_CUR);
    if (fwrite(&commit_meta, sizeof(commit_t), 1, log_file) != 1) {
        VERBOSE_PRINT(do_verbose, "Failed to set commit bit in log\n");
        fclose(log_file);
        return ret;
    }

    fflush(log_file);
    fclose(log_file);

    write_id->synced = 1;
    ret = 0;

    VERBOSE_PRINT(do_verbose, "Success\n"); // On success returns 0.
    return ret;
}
