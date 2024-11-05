#include "../src/gtfs.hpp"
#include <pthread.h>

// Assumes files are located within the current directory
string directory;
int verbose;

// To not keep 'failed' data from previous tests
void cleanup_test_files() {
    char cwd[256];
    if (getcwd(cwd, sizeof(cwd)) == NULL) {
        cout << "Failed to get current directory\n";
        return;
    }
    
    DIR* dir = opendir(cwd);
    if (!dir) {
        cout << "Failed to open directory for cleanup\n";
        return;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        string fname = entry->d_name;
        if (fname == "." || fname == "..") continue;

        if (fname.length() > 4 && 
            (fname.substr(fname.length() - 4) == ".txt" || 
             fname.substr(fname.length() - 4) == ".log")) {
            string full_path = string(cwd) + "/" + fname;
            if (remove(full_path.c_str()) != 0) {
                cout << "Failed to remove file: " << full_path << "\n";
            } else {
                cout << "Cleaned up file: " << full_path << "\n";
            }
        }
    }
    closedir(dir);

    string pid_file = string(cwd) + "/crash_pid.txt";
    remove(pid_file.c_str());
}

// **Test 1**: Testing that data written by one process is then successfully read by another process.
void writer() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test1.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Hi, I'm the writer.\n";
    write_t *wrt = gtfs_write_file(gtfs, fl, 10, str.length(), str.c_str());
    gtfs_sync_write_file(wrt);

    gtfs_close_file(gtfs, fl);
}

void reader() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test1.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Hi, I'm the writer.\n";
    char *data = gtfs_read_file(gtfs, fl, 10, str.length());
    if (data != NULL) {
        str.compare(string(data)) == 0 ? cout << PASS << "\n" : cout << FAIL << " Data is: " << data << "(END)\n";
    } else {
        cout << FAIL << " Data is null!\n";
    }
    gtfs_close_file(gtfs, fl);
}

void test_write_read() {
    int pid;
    pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(-1);
    }
    if (pid == 0) {
        writer();
        exit(0);
    }
    waitpid(pid, NULL, 0);
    reader();
}

// **Test 2**: Testing that aborting a write returns the file to its original contents.
void test_abort_write() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test2.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_abort_write_file(wrt2);

    char *data1 = gtfs_read_file(gtfs, fl, 0, str.length());
    if (data1 != NULL) {
        // First write was synced so reading should be successful
        if (str.compare(string(data1)) != 0) {
            cout << FAIL << "\n";
        } else {
            // Second write was aborted and there was no string written in that offset
            char *data2 = gtfs_read_file(gtfs, fl, 20, str.length());
            if (data2 == NULL) {
                cout << FAIL << "\n";
            } else if (string(data2).compare("") == 0) {
                cout << PASS << "\n";
            }
        }
    } else {
        cout << FAIL << "\n";
    }
    gtfs_close_file(gtfs, fl);
}

// **Test 3**: Testing that the logs are truncated.
void test_truncate_log() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test3.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_sync_write_file(wrt2);

    cout << "Before GTFS cleanup\n";
    system("ls -l .");

    gtfs_clean(gtfs);

    cout << "After GTFS cleanup\n";
    system("ls -l .");

    cout << "If log is truncated: " << PASS << " If exactly same output: " << FAIL << "\n";

    gtfs_close_file(gtfs, fl);

}

void test_multiple_writes() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test4.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Hello There!";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_sync_write_file(wrt2);

    gtfs_close_file(gtfs, fl);

    // Now test out the reading
    fl = gtfs_open_file(gtfs, filename, 100);

    char *data_0 = gtfs_read_file(gtfs, fl, 0, str.length());

    if (data_0 != NULL) {
        str.compare(string(data_0)) == 0 ? cout << PASS << "\n" : cout << FAIL << " Data is: " << data_0 << "(END)\n";
    } else {
        cout << FAIL << " Data is null!\n";
    }

    char *data_1 = gtfs_read_file(gtfs, fl, 20, str.length());

    if (data_1 != NULL) {
        str.compare(string(data_1)) == 0 ? cout << PASS << "\n" : cout << FAIL << " Data is: " << data_1 << "(END)\n";
    } else {
        cout << FAIL << " Data is null!\n";
    }

    gtfs_close_file(gtfs, fl);
}


// Test 5: Verifying that only synced writes survive crashes
void crash_writer() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test5.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 200);
    
    // Write #1 - synced, should survive
    const char *data1 = "Synced write #1\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, strlen(data1), data1);
    if (gtfs_sync_write_file(wrt1) <= 0) {
        cout << "First sync failed!\n";
    }
    
    // Write #2 - not synced, should be lost
    const char *data2 = "Unsynced write\n";
    write_t *wrt2 = gtfs_write_file(gtfs, fl, 50, strlen(data2), data2);
    
    abort();
}

void recovery_verifier() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test5.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 200);
    
    cout << "After recovery, file contents:\n";
    char debug_buf[201];
    memcpy(debug_buf, fl->data, 200);
    debug_buf[200] = '\0';
    cout << debug_buf << endl;
    
    // Check write #1 - should be recovered
    const char *expected1 = "Synced write #1\n";
    char *read1 = gtfs_read_file(gtfs, fl, 0, strlen(expected1));
    cout << "Read1 data: " << (read1 ? read1 : "null") << endl;
    if (read1 && strcmp(read1, expected1) == 0) {
        cout << "First synced write recovered correctly: " << PASS << "\n";
    } else {
        cout << "First synced write not recovered: " << FAIL << "\n";
    }
    
    // Check write #2 - should be empty
    char *read2 = gtfs_read_file(gtfs, fl, 50, 20);
    cout << "Read2 data: " << (read2 ? read2 : "null") << endl;
    if (read2) {
        bool is_empty = true;
        for (size_t i = 0; i < 20; i++) {
            if (read2[i] != '\0') {
                is_empty = false;
                break;
            }
        }
        if (is_empty) {
            cout << "Unsynced write properly discarded: " << PASS << "\n";
        } else {
            cout << "Unsynced write persisted: " << FAIL << "\n";
            cout << "Found data: " << read2 << endl;
        }
    }
    
    gtfs_close_file(gtfs, fl);
}

void test_crash_recovery() {
    int pid = fork();
    if (pid == 0) {
        crash_writer();
        exit(0);
    }
    waitpid(pid, NULL, 0);
    recovery_verifier();
}



 // Test 6: To verify that the system handles concurrent access during log cleaning and can recover from a crash during log cleaning

// Write multiple times to file
void writer_process_for_cleaning_test() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test6.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    for (int i = 0; i < 5; ++i) {
        char buf[32];
        snprintf(buf, sizeof(buf), "Data segment %d\n", i);
        write_t *wrt = gtfs_write_file(gtfs, fl, i * 20, strlen(buf), buf);
        gtfs_sync_write_file(wrt);
        usleep(50000); // Sleep to simulate time taken for writing
    }

    gtfs_close_file(gtfs, fl);
}

// Attempt to clean while writer is active
void cleaner_process() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    int ret = gtfs_clean(gtfs);
    if (ret == 0) {
        cout << "Log cleaning completed successfully during active writes: " << PASS << "\n";
    } else {
        cout << "Log cleaning failed during active writes: " << FAIL << "\n";
    }
}

// Child process attempts clean and crashes immediately, then parent retries clean - should succeed
void crash_during_cleaning_process() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);

    pid_t pid = fork();
    if (pid == 0) {
        gtfs_clean(gtfs);
        abort(); // Simulate crash
    } else {
        waitpid(pid, NULL, 0);

        gtfs_clean(gtfs);

        string filename = "test6.txt";
        file_t *fl = gtfs_open_file(gtfs, filename, 100);

        bool data_intact = true;
        for (int i = 0; i < 5; ++i) {
            char expected[32];
            snprintf(expected, sizeof(expected), "Data segment %d\n", i);
            char *read_data = gtfs_read_file(gtfs, fl, i * 20, strlen(expected));
            if (!read_data || strcmp(read_data, expected) != 0) {
                data_intact = false;
                break;
            }
        }

        if (data_intact) {
            cout << "Data integrity maintained after crash during log cleaning: " << PASS << "\n";
        } else {
            cout << "Data corruption detected after crash during log cleaning: " << FAIL << "\n";
        }

        gtfs_close_file(gtfs, fl);
    }
}

// Calls writer and cleaner processes, then calls crash_during_cleaning_process to checkout output
void test_concurrent_log_cleaning_and_crash() {
    int writer_pid = fork();
    if (writer_pid == 0) {
        writer_process_for_cleaning_test();
        exit(0);
    }

    // Give the writer a head start
    usleep(100000);

    // Start cleaner process
    int cleaner_pid = fork();
    if (cleaner_pid == 0) {
        cleaner_process();
        exit(0);
    }

    waitpid(writer_pid, NULL, 0);
    waitpid(cleaner_pid, NULL, 0);

    crash_during_cleaning_process();
}


// Test 7: Verifying that the file system prevents multiple processes from opening the same file concurrently
// NOTE: Only reason that acquire lock is non-blocking in open is to show that two files cannot access at the same time - could theoretically be changed
void open_file_in_process(int id, int *result) {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test7.txt";

    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    if (fl != NULL) {
        // Keep the file open for a while
        sleep(2);
        gtfs_close_file(gtfs, fl);
        *result = 0;
    } else {
        *result = -1;
    }
}

void test_single_process_file_open() {
    int pid1 = fork();
    if (pid1 == 0) {
        int result = -1;
        open_file_in_process(1, &result);
        exit(result == 0 ? 0 : 1);
    }

    // Give the first process a moment to open the file
    usleep(500000); // 0.5 seconds

    int pid2 = fork();
    if (pid2 == 0) {
        int result = -1;
        open_file_in_process(2, &result);
        exit(result == 0 ? 0 : 1);
    }

    int status1, status2;
    waitpid(pid1, &status1, 0);
    waitpid(pid2, &status2, 0);

    int res1 = WEXITSTATUS(status1);
    int res2 = WEXITSTATUS(status2);

    if (res1 == 0 && res2 != 0) {
        cout << "Second process correctly failed to open an already open file: " << PASS << "\n";
    } else {
        cout << "Second process incorrectly allowed to open an already open file: " << FAIL << "\n";
    }
}

// Test 8: To verify that the file system handles concurrent access within a process / threading correctly
#include <pthread.h> 

struct thread_arg_t {
    int thread_id;
    file_t *fl;
};

void *thread_write(void *arg) {
    thread_arg_t *thread_arg = (thread_arg_t *)arg;
    int thread_id = thread_arg->thread_id;
    file_t *fl = thread_arg->fl;

    // Each thread writes to a different region
    int offset = thread_id * 50;
    string data = "Thread " + to_string(thread_id) + " data\n";

    write_t *wrt = gtfs_write_file(fl->file_gtfs, fl, offset, data.length(), data.c_str());
    if (wrt != NULL) {
        gtfs_sync_write_file(wrt);
    } else {
        cout << "Thread " << thread_id << " failed to write: " << FAIL << "\n";
    }

    return NULL;
}

void test_multi_threaded_access() {
    const int NUM_THREADS = 4;
    pthread_t threads[NUM_THREADS];
    thread_arg_t thread_args[NUM_THREADS];

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test8.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 200);

    if (!fl) {
        cout << "Failed to open file for multi-threaded access test: " << FAIL << "\n";
        return;
    }

    // Create threads
    for (int i = 0; i < NUM_THREADS; ++i) {
        thread_args[i].thread_id = i;
        thread_args[i].fl = fl;

        if (pthread_create(&threads[i], NULL, thread_write, &thread_args[i]) != 0) {
            cout << "Failed to create thread " << i << ": " << FAIL << "\n";
            gtfs_close_file(gtfs, fl);
            return;
        }
    }

    // Join threads
    for (int i = 0; i < NUM_THREADS; ++i) {
        pthread_join(threads[i], NULL);
    }

    // Verify data integrity
    bool data_intact = true;
    for (int i = 0; i < NUM_THREADS; ++i) {
        int offset = i * 50;
        string expected = "Thread " + to_string(i) + " data\n";
        char *read_data = gtfs_read_file(gtfs, fl, offset, expected.length());
        if (!read_data || expected != string(read_data)) {
            data_intact = false;
            cout << "Data mismatch in thread " << i << ": " << FAIL << "\n";
        } else {
        }
        if (read_data) {
            free(read_data);
        }
    }

    if (data_intact) {
        cout << "Data integrity maintained with multi-threaded access: " << PASS << "\n";
    }

    gtfs_close_file(gtfs, fl);
}

// Test 9: Testing various edge cases specified in the project description
// 9a: Writing Beyond the End of the File
void test_write_beyond_file_length() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test9.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 50); // File length is 50 bytes

    string data = "This is test data";
    int offset = 40;
    int length = 20; 

    write_t *wrt = gtfs_write_file(gtfs, fl, offset, length, data.c_str());
    if (wrt == NULL) {
        cout << "Write beyond file length correctly rejected: " << PASS << "\n";
    } else {
        cout << "Write beyond file length incorrectly allowed: " << FAIL << "\n";
    }

    gtfs_close_file(gtfs, fl);
}

// 9b: Reading from an unwritten offset
void test_read_unwritten_offset() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test9.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 50);

    int offset = 10;
    int length = 20;
    char *data = gtfs_read_file(gtfs, fl, offset, length);
    if (data && string(data) == "") {
        cout << "Reading unwritten offset returns empty string: " << PASS << "\n";
    } else {
        cout << "Reading unwritten offset did not return empty string: " << FAIL << "\n";
    }

    gtfs_close_file(gtfs, fl);
}

// 9c: Opening a file with a smaller file_length than existing file length
void test_open_with_smaller_file_length() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test9.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    gtfs_close_file(gtfs, fl);

    fl = gtfs_open_file(gtfs, filename, 50);
    if (fl == NULL) {
        cout << "Opening file with smaller length correctly rejected: " << PASS << "\n";
    } else {
        cout << "Opening file with smaller length incorrectly allowed: " << FAIL << "\n";
        gtfs_close_file(gtfs, fl);
    }
}

// 9d: Removing an open file
void test_remove_open_file() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test9.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    int ret = gtfs_remove_file(gtfs, fl);
    if (ret == -1) {
        cout << "Removing an open file correctly rejected: " << PASS << "\n";
    } else {
        cout << "Removing an open file incorrectly allowed: " << FAIL << "\n";
    }

    gtfs_close_file(gtfs, fl);

    ret = gtfs_remove_file(gtfs, fl);
    if (ret == 0) {
        cout << "Removing a closed file succeeded: " << PASS << "\n";
    } else {
        cout << "Removing a closed file failed: " << FAIL << "\n";
    }
}

// Test 10: To verify that the file system can recover after multiple crashes
void write_and_crash(int stage) {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test10.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 200);

    string data1 = "Initial data\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, data1.length(), data1.c_str());
    gtfs_sync_write_file(wrt1);

    if (stage == 1) abort(); // Crash after first write

    string data2 = "Second data\n";
    write_t *wrt2 = gtfs_write_file(gtfs, fl, 50, data2.length(), data2.c_str());
    gtfs_sync_write_file(wrt2);

    if (stage == 2) abort(); // Crash after second write

    string data3 = "Third data\n";
    write_t *wrt3 = gtfs_write_file(gtfs, fl, 100, data3.length(), data3.c_str());
    gtfs_sync_write_file(wrt3);

    if (stage == 3) abort(); // Crash after third write

    gtfs_close_file(gtfs, fl);
}

void test_multiple_sequential_crashes() {
    for (int stage = 1; stage <= 3; ++stage) {
        pid_t pid = fork();
        if (pid == 0) {
            write_and_crash(stage);
            exit(0);
        } else {
            waitpid(pid, NULL, 0);

            // Attempt recovery
            gtfs_t *gtfs = gtfs_init(directory, verbose);
            gtfs_clean(gtfs);

            // Verify data integrity
            string filename = "test10.txt";
            file_t *fl = gtfs_open_file(gtfs, filename, 200);

            string data1 = "Initial data\n";
            char *read1 = gtfs_read_file(gtfs, fl, 0, data1.length());
            bool data1_ok = read1 && data1 == string(read1);

            string data2 = "Second data\n";
            char *read2 = gtfs_read_file(gtfs, fl, 50, data2.length());
            bool data2_ok = (stage >= 2) ? (read2 && data2 == string(read2)) : true;

            string data3 = "Third data\n";
            char *read3 = gtfs_read_file(gtfs, fl, 100, data3.length());
            bool data3_ok = (stage >= 3) ? (read3 && data3 == string(read3)) : true;

            if (data1_ok && data2_ok && data3_ok) {
                cout << "Data integrity maintained after crash at stage " << stage << ": " << PASS << "\n";
            } else {
                cout << "Data corruption detected after crash at stage " << stage << ": " << FAIL << "\n";
            }

            gtfs_close_file(gtfs, fl);
        }
    }
}

int main(int argc, char **argv) {
    cleanup_test_files();

    if (argc < 2)
        printf("Usage: ./test verbose_flag\n");
    else
        verbose = strtol(argv[1], NULL, 10);

    char cwd[256];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        directory = string(cwd);
    } else {
        cout << "[cwd] Something went wrong.\n";
    }

    cout << "================== Test 1 ==================\n";
    cout << "Testing that data written by one process is then successfully read by another process.\n";
    test_write_read();

    cout << "================== Test 2 ==================\n";
    cout << "Testing that aborting a write returns the file to its original contents.\n";
    test_abort_write();

    cout << "================== Test 3 ==================\n";
    cout << "Testing that the logs are truncated.\n";
    test_truncate_log();

    cout << "================== Test 4 ==================\n";
    cout << "Testing multiple writes\n";
    test_multiple_writes();

    cout << "================== Test 5 ==================\n";
    cout << "Testing crash recovery with pending logs\n";
    test_crash_recovery();

    cout << "================== Test 6 ==================\n";
    cout << "Testing concurrent log operations and crash during log cleaning\n";
    test_concurrent_log_cleaning_and_crash();

    cout << "================== Test 7 ==================\n";
    cout << "Testing that a file cannot be opened by multiple processes concurrently\n";
    test_single_process_file_open();

    cout << "================== Test 8 ==================\n";
    cout << "Testing multi-threaded access within a process\n";
    test_multi_threaded_access();

    cout << "================== Test 9 ==================\n";
    cout << "Testing edge cases for file operations\n";
    test_write_beyond_file_length();
    test_read_unwritten_offset();
    test_open_with_smaller_file_length();
    test_remove_open_file();

    cout << "================== Test 10 ==================\n";
    cout << "Testing recovery after multiple sequential crashes\n";
    test_multiple_sequential_crashes();

    return 0;
}
