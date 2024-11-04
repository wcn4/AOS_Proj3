#include "gtfs.hpp"

#define VERBOSE_PRINT(verbose, str...) do { \
    if (verbose) cout << "VERBOSE: "<< __FILE__ << ":" << __LINE__ << " " << __func__ << "(): " << str; \
} while(0)



int do_verbose;

// Helper function to acquire a file lock
bool acquire_lock(int fd, bool block) {
    /*
    if (flock(fd, LOCK_EX) != 0) {
        return false;
    }
    return true;
    */
   int cmd = block ? F_ULOCK : F_ULOCK | F_TLOCK;
   if (lockf(fd, cmd, 0) != 0) {
        return false;
    }
    return true;
}

// Helper function to release a file lock
bool release_lock(int fd) {
    /*
    if (flock(fd, LOCK_UN) != 0) {
        return false;
    }
    return true;
    */
   if (lockf(fd, F_ULOCK, 0) != 0) {
        return false;
    }
    return true;
}

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
    #ifdef USE_LOGS_DIR
    return dirname + "/.logs/" + filename + ".log";
    #else

    #ifdef HIDDEN_LOGS
    return dirname + "/." + filename + ".log";
    #else
    return dirname + "/" + filename + ".log";
    #endif
    
    #endif
}


// Helper function to apply logs 

// Helper function to apply logs to a specific file
bool apply_log(const string& directory, const string& filename, bool lock_held) {
    string log_path = get_log_path(directory, filename);
    string file_path = directory + "/" + filename;


    // Do this to acquire the lock
    int fd = open(file_path.c_str(), O_RDWR);
    if (fd == -1) {
        VERBOSE_PRINT(do_verbose, "Target file " << file_path << " does not exist or cannot be opened.");
        return false;
    }

    // Acquire an exclusive lock on the target file
    VERBOSE_PRINT(do_verbose, "Attempting to acquire lock for " << filename << " \n");
    if (!lock_held && !acquire_lock(fd, false)) {
        VERBOSE_PRINT(do_verbose, "Failed to acquire lock on file " << file_path << " \n");
        close(fd);
        return false;
    }

    // Open the log file in binary read mode
    FILE* log_file = fopen(log_path.c_str(), "rb");
    if (!log_file) {
        VERBOSE_PRINT(do_verbose, "Log file " << log_path << " does not exist or cannot be opened.\n");
        return false;
    }

    // Open the target file in binary read-write mode
    FILE* fp = fopen(file_path.c_str(), "rb+");
    if (!fp) {
        VERBOSE_PRINT(do_verbose, "Target file " << file_path << " does not exist or cannot be opened.\n");
        fclose(log_file);
        return false;
    }

    commit_t commit_meta;
    // Each loop, read the meta data for one commit from the log file
    while (fread(&commit_meta, sizeof(commit_t), 1, log_file) == 1) {
        if (commit_meta.commited) {
            // Allocate buffer to read the data associated with this commit
            char* buffer = new char[commit_meta.length];

            //Read from log after metadata is read
            size_t bytes_read = fread(buffer, sizeof(char), commit_meta.length, log_file);

            //Verify that the amount read is what we expect
            if (bytes_read != static_cast<size_t>(commit_meta.length)) {
                VERBOSE_PRINT(do_verbose, "Failed to read the expected amount of data from log " << log_path << " . Expected " << commit_meta.length << " bytes, got " << bytes_read << " bytes.\n");
                VERBOSE_PRINT(do_verbose, "Data read: " << buffer << "(END)");
                delete[] buffer;
                fclose(log_file);
                //remove(log_path.c_str()); // Remove corrupted logs (prevent future logs from being corrupted)
                fclose(fp);
                return false;
            }
            
            // Seek to the specified offset in the target file
            if (fseek(fp, commit_meta.offset, SEEK_SET) != 0) {
                VERBOSE_PRINT(do_verbose, "Failed to seek to offset " << commit_meta.offset << " in file " << file_path << " .\n");
                delete[] buffer;
                fclose(log_file);
                remove(log_path.c_str());
                fclose(fp);
                return false;
            }

            // Write the data from the log to the target file
            size_t bytes_written = fwrite(buffer, sizeof(char), commit_meta.length, fp);
            if (bytes_written != static_cast<size_t>(commit_meta.length)) {
                VERBOSE_PRINT(do_verbose, "Failed to write data to file " << file_path << " at offset " << commit_meta.offset << " .\n");
                delete[] buffer;
                fclose(log_file);
                fclose(fp);
                return false;
            }

            // Flush the changes to ensure data is written to disk
            if (fflush(fp) != 0) {
                VERBOSE_PRINT(do_verbose, "Failed to flush data to file " << file_path << " after writing.\n");
                delete[] buffer;
                fclose(log_file);
                fclose(fp);
                return false;
            }

            delete[] buffer;

            if (do_verbose) {
                VERBOSE_PRINT(do_verbose, "Applied commit to file " << file_path << " at offset " << commit_meta.offset << " for " << commit_meta.length << " bytes.\n");
            }
            
        } else {
            // If the commit is not marked as committed, skip the data
            VERBOSE_PRINT(do_verbose, "Skipping uncommitted data. Moving reader by " << commit_meta.length << " bytes. \n");
            if (fseek(log_file, commit_meta.length, SEEK_CUR) != 0) {
                VERBOSE_PRINT(do_verbose, "Failed to skip uncommitted data in log " << log_path << " .\n");
                fclose(log_file);
                fclose(fp);
                return false;
            }
        }
    }


    // Release the lock and close the file descriptor
    if (!release_lock(fd)) {
        std::cerr << "Failed to release lock on file " << file_path << std::endl;
        close(fd);
        return false;
    }
    close(fd);

    fclose(log_file);
    fclose(fp);

    //Delete the Log file after we are done
    //log_file = fopen(log_path.c_str(), "wb");
    //if (log_file) fclose(log_file);
    remove(log_path.c_str());

    if (do_verbose) {
        VERBOSE_PRINT(do_verbose, "Successfully applied logs from " << log_path << " to file " << file_path << " .\n");
    }

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

    #ifdef USE_LOGS_DIR
    // Create .logs directory
    string logs_dir = directory + "/.logs";
    if (!create_directory(logs_dir)) {
        VERBOSE_PRINT(do_verbose, "Failed to create or access logs directory " << logs_dir << "\n");
        return NULL;
    }
    #endif

    /* Later, replace this such that it checked shared memory first to look for a gtfs instance*/
    // Initialize gtfs struct
    gtfs = new gtfs_t();
    gtfs->dirname = directory;

    /* Should initialize a way to keep track of open file structs to make implementations of clean and abort simpler*/

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

    #ifdef USE_LOGS_DIR
    string logs_dir = gtfs->dirname + "/.logs";
    DIR* dir = opendir(logs_dir.c_str());
    if (!dir) {
        VERBOSE_PRINT(do_verbose, "Failed to open logs directory " << logs_dir << "\n");
        return ret;
    }
    #else
    string logs_dir = gtfs->dirname;
    DIR* dir = opendir(gtfs->dirname.c_str());
    if (!dir) {
        VERBOSE_PRINT(do_verbose, "Failed to open " << gtfs->dirname << " during clean \n");
        return ret;
    }
    #endif

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        string log_fname = entry->d_name;
        if (log_fname == "." || log_fname == "..") continue;

        #ifndef USE_LOGS_DIR
        // Skip files that don't end with ".log"
        if (log_fname.length() < 4 || log_fname.substr(log_fname.length() - 4) != ".log") {
            continue;
        }
        #endif

        string log_path = logs_dir + "/" + log_fname;
        // Extract original filename
        #ifdef USE_LOGS_DIR
        string original_fname = log_fname.substr(0, log_fname.length() - 4);
        #else

        #ifdef HIDDEN_LOGS
        string original_fname = log_fname.substr(1, log_fname.length() - 5); // Start from one to avoid the . and end is inclusive
        #else
        string original_fname = log_fname.substr(0, log_fname.length() - 4); // If not including a prefix at the front
        #endif

        #endif

        if (!apply_log(gtfs->dirname, original_fname, false)) {
            VERBOSE_PRINT(do_verbose, "Failed to apply log for " << original_fname << " during clean!\n");
            VERBOSE_PRINT(do_verbose, "Log file: " << log_fname << "\n");
            return ret;
        }
    
    }
    ret = 0;
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

    //This is to access the file and make sure another process isn't cleaning / applying logs
    if (!acquire_lock(fd, true)) {
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

    // Apply existing logs
    /* Should double check this portion */
    struct stat log_st;
    if (stat(log_path.c_str(), &log_st) == 0) {
        VERBOSE_PRINT(do_verbose, "Detecting logs from previous instance, recovering data\n");
        apply_log(gtfs->dirname, filename, true);
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
    fl->gtfs = gtfs;
    fl->fd = fd;
    fl->file_length = file_length;
    fl->data = data;
    fl->log_path = log_path;
    //fl->sem_name = sem_name;

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

    struct stat log_st;
    if (stat(log_path.c_str(), &log_st) == 0) {
        VERBOSE_PRINT(do_verbose, "Detecting logs from previous instance, recovering data\n");
        // Clean to apply any pending logs
        if (!apply_log(gtfs->dirname, fl->filename, true)) {
            VERBOSE_PRINT(do_verbose, "Failed to apply logs during close\n");
            return ret;
        }
    }
    
    // Unmap the file
    if (munmap(fl->data, fl->file_length) != 0) {
        VERBOSE_PRINT(do_verbose, "Failed to unmap file " << fl->filename << "\n");
        return ret;
    }

    release_lock(fl->fd);

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

    // Check that read is valid
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

    VERBOSE_PRINT(do_verbose, "Value read: " << ret_data << "(END)\n");

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

    write_id = new write_t();
    write_id->filename = fl->filename;
    write_id->fl = fl;
    write_id->offset = offset;
    write_id->length = length;
    write_id->data = (char*)malloc(length);
    write_id->synced = 0;
    write_id->aborted = 0;
    write_id->old_data = (char*)malloc(length);

    if (write_id->data == NULL || write_id->old_data == NULL) {
        VERBOSE_PRINT(do_verbose, "Could not allocate memory for write struct\n");
        free(write_id->data);
        free(write_id->old_data);
        free(write_id);
        return NULL;
    }
    // Copy data over to the write struct
    memcpy(write_id->data, data, length);
    memcpy(write_id->old_data, fl->data + offset, length);

    //Write data to the in memory copy
    memcpy(fl->data + offset, data, length);

    VERBOSE_PRINT(do_verbose, "Value written: " << data << "(END)\n");

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

    /* May need to include a reference to gtfs to have syncs work*/
    /* Assume this works for now*/

    gtfs_t *gtfs = write_id->fl->gtfs;
    file_t *fl = write_id->fl;
    
    string log_path = get_log_path(gtfs->dirname, write_id->filename);

    /* Need to acquire or spin until lock is obtained*/

    //FILE* log_file = fopen(log_path.c_str(), "ab");

    //Open for both r/w in binary mode
    FILE* log_file = fopen(log_path.c_str(), "rb+");

    //Do safety checks, create file if it doesn't exist already
    if (!log_file) {
        log_file = fopen(log_path.c_str(), "wb+");
        if (!log_file) {
            VERBOSE_PRINT(do_verbose, "Failed to open log file during sync. \n");
            fclose(log_file);
        return -1;
        }
    }
    

    // Go to the end of the log file
    if(fseek(log_file, 0, SEEK_END) != 0) {
        VERBOSE_PRINT(do_verbose, "Failed to seek to the end of the log!\n");
        fclose(log_file);
        return -1;
    }

    commit_t commit_meta;
    commit_meta.offset = write_id->offset;
    commit_meta.length = write_id->length;
    commit_meta.commited = 0;

    VERBOSE_PRINT(do_verbose, "Size of commit: " << sizeof(commit_t) <<" bytes!\n");

    // Write commit metadata to log
    if (fwrite(&commit_meta, sizeof(commit_t), 1, log_file) != 1) {
        VERBOSE_PRINT(do_verbose, "Failed to write commit metadata\n");
        fclose(log_file);
        return -1;
    } else {
        VERBOSE_PRINT(do_verbose, "Wrote commit metadata to log\n");
        VERBOSE_PRINT(do_verbose, "Commit metadata. Offset: " << commit_meta.offset << " length: " << commit_meta.length << "\n");
    }

    //Write the data to the log
    if (fwrite(write_id->data, sizeof(char), write_id->length, log_file) != (size_t)write_id->length) {
        VERBOSE_PRINT(do_verbose, "Failed to write data to log\n");
        fclose(log_file);
        return -1;
    } else {
        VERBOSE_PRINT(do_verbose, "Wrote data to log. Data: " << write_id->data << "(END)\n");
    }

    //Ensure that the commit is written to disk
    fflush(log_file);

    commit_meta.commited = 1;

    //Set pointer back to the start of the commit message
    fseek(log_file, -((long)write_id->length + sizeof(commit_t)), SEEK_CUR);
    if (fwrite(&commit_meta, sizeof(commit_t), 1, log_file) != 1) {
        VERBOSE_PRINT(do_verbose, "Failed to set commit bit in log\n");
        fclose(log_file);
        return -1;
    } else {
        VERBOSE_PRINT(do_verbose, "Set commit bit within log\n");
    }

    fflush(log_file);
    fclose(log_file);

    write_id->synced = 1;
    ret = write_id->length; // Set return code to the number of bytes written


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
    if (write_id->fl == NULL) {
        VERBOSE_PRINT(do_verbose, "Write_id file does not exist\n");
    }

    if (write_id->synced) {
        VERBOSE_PRINT(do_verbose, "Cannot abort a write that has been synced!\n");
        return ret;
    }
    
    file_t *fl = write_id->fl;
    memcpy(fl->data + write_id->offset, write_id->old_data, write_id->length);
    write_id->aborted = 1;
    ret = 0;

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

