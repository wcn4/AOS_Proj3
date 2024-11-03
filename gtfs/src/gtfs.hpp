#ifndef GTFS
#define GTFS

#include <string>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <unistd.h>
#include <sys/wait.h>

// New libraries:

#include <sys/stat.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <cstring>
#include <dirent.h>
#include <errno.h>

using namespace std;

#define PASS "\033[32;1m PASS \033[0m\n"
#define FAIL "\033[31;1m FAIL \033[0m\n"

// GTFileSystem basic data structures 

#define MAX_FILENAME_LEN 255
#define MAX_NUM_FILES_PER_DIR 1024

extern int do_verbose;

typedef struct gtfs {
    string dirname;
    // TODO: Add any additional fields if necessary
} gtfs_t;

typedef struct file {
    string filename;
    int file_length;
    // TODO: Add any additional fields if necessary
    char *data; // In memory copy of the data
    
    //Log file path
    int fd; // This is to allow OS flocks to be acquired and released
    string log_path;
    gtfs *gtfs; //This is to simplify sync implementation

} file_t;

typedef struct write {
    string filename;
    int offset;
    int length;
    char *data;
    // TODO: Add any additional fields if necessary

    file_t *fl; // Allows for easy abort implementation
    int synced; // 0: not synced, 1: synced
    int aborted; // 0: not aborted, 1: aborted
    char *old_data; // old data before write
} write_t;

typedef struct log_meta {
    int length; // Total length of the log file
    int num_commits;
} log_meta_t;

// Want a small commit metadata struct
typedef struct commit {
    int offset;
    int length;
    int commited; // 0: not commited, 1: commited (this is to ensure commits are not corrupted)
} commit_t;

// GTFileSystem basic API calls

gtfs_t* gtfs_init(string directory, int verbose_flag);
int gtfs_clean(gtfs_t *gtfs);

file_t* gtfs_open_file(gtfs_t* gtfs, string filename, int file_length);
int gtfs_close_file(gtfs_t* gtfs, file_t* fl);
int gtfs_remove_file(gtfs_t* gtfs, file_t* fl);

char* gtfs_read_file(gtfs_t* gtfs, file_t* fl, int offset, int length);
write_t* gtfs_write_file(gtfs_t* gtfs, file_t* fl, int offset, int length, const char* data);
int gtfs_sync_write_file(write_t* write_id);
int gtfs_abort_write_file(write_t* write_id);

// BONUS: Implement below API calls to get bonus credits

int gtfs_clean_n_bytes(gtfs_t *gtfs, int bytes);
int gtfs_sync_write_file_n_bytes(write_t* write_id, int bytes);

// TODO: Add here any additional data structures or API calls


#endif
