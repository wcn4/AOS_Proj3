#include "gtfs.hpp"

#define VERBOSE_PRINT(verbose, str...) do { \
    if (verbose) cout << "VERBOSE: "<< __FILE__ << ":" << __LINE__ << " " << __func__ << "(): " << str; \
} while(0)

int do_verbose;

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
    gtfs_t *gtfs = NULL;
    VERBOSE_PRINT(do_verbose, "Initializing GTFileSystem inside directory " << directory << "\n");
    //TODO: Add any additional initializations and checks, and complete the functionality

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

    /* Later, replace this such that it checked shared memory first to look for a gtfs instance*/
    // Initialize gtfs struct
    gtfs = new gtfs_t();
    gtfs->dirname = directory;

    // Iterate through files in the directory and initialize logs
    DIR* dir = opendir(directory.c_str());
    if (!dir) {
        VERBOSE_PRINT(do_verbose, "Failed to open directory " << directory << "\n");
        delete gtfs;
        return NULL;
    }

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns non NULL.
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
    //TODO: Add any additional initializations and checks, and complete the functionality

    /* This implementation should eventually use whatever datastructure within gtfs supports multiple file_t structs */

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
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
    /* Should double check this portion */
    struct stat log_st;
    if (stat(log_path.c_str(), &log_st) == 0) {
        apply_logs(log_path, fl);
    }

    /* Delete this comment */
    // Close the file descriptor (lock remains held)
    // Note: Need to keep the fd open to maintain the lock
    //close(fd);

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns non NULL.
    return fl;
}

int gtfs_close_file(gtfs_t* gtfs, file_t* fl) {
    int ret = -1;
    if (gtfs and fl) {
        VERBOSE_PRINT(do_verbose, "Closing file " << fl->filename << " inside directory " << gtfs->dirname << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem or file does not exist\n");
        return ret;
    }
    //TODO: Add any additional initializations and checks, and complete the functionality


    string log_path = get_log_path(gtfs->dirname, fl->filename);

    // Clean to apply any pending logs
    if (!apply_logs(log_path, fl)) {
        VERBOSE_PRINT(do_verbose, "Failed to apply logs during close\n");
        return ret;
    }

    // Unmap the file
    if (munmap(fl->data, fl->file_length) != 0) {
        VERBOSE_PRINT(do_verbose, "Failed to unmap file " << fl->filename << "\n");
        return ret;
    }

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
    return ret;
}

int gtfs_remove_file(gtfs_t* gtfs, file_t* fl) {
    int ret = -1;
    if (gtfs and fl) {
        VERBOSE_PRINT(do_verbose, "Removing file " << fl->filename << " inside directory " << gtfs->dirname << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem or file does not exist\n");
        return ret;
    }
    //TODO: Add any additional initializations and checks, and complete the functionality

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

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
    return ret;
}

char* gtfs_read_file(gtfs_t* gtfs, file_t* fl, int offset, int length) {
    char* ret_data = NULL;
    if (gtfs and fl) {
        VERBOSE_PRINT(do_verbose, "Reading " << length << " bytes starting from offset " << offset << " inside file " << fl->filename << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem or file does not exist\n");
        return NULL;
    }
    //TODO: Add any additional initializations and checks, and complete the functionality

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns pointer to data read.
    return ret_data;
}

write_t* gtfs_write_file(gtfs_t* gtfs, file_t* fl, int offset, int length, const char* data) {
    write_t *write_id = NULL;
    if (gtfs and fl) {
        VERBOSE_PRINT(do_verbose, "Writting " << length << " bytes starting from offset " << offset << " inside file " << fl->filename << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem or file does not exist\n");
        return NULL;
    }
    //TODO: Add any additional initializations and checks, and complete the functionality

    //Modify in memmory copy of the file but not the actual file

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns non NULL.
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
    //TODO: Add any additional initializations and checks, and complete the functionality

    // Writes the commit to the log
    // Maybe store a strict metadata struct?

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns number of bytes written.
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
    //TODO: Add any additional initializations and checks, and complete the functionality

    VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns 0.
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

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
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

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
    return ret;
}

