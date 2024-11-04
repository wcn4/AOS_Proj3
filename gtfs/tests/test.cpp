#include "../src/gtfs.hpp"

// Assumes files are located within the current directory
string directory;
int verbose;

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
        //str.compare(string(data)) == 0 ? cout << PASS : cout << FAIL;

        str.compare(string(data)) == 0 ? cout << PASS : cout << FAIL << " Data is: " << data << "(END)\n";
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
        // First write was synced so reading should be successfull
        if (str.compare(string(data1)) != 0) {
            cout << FAIL;
        }
        // Second write was aborted and there was no string written in that offset
        char *data2 = gtfs_read_file(gtfs, fl, 20, str.length());
        if (data2 == NULL) {
            cout << FAIL;
        } else if (string(data2).compare("") == 0) {
            cout << PASS;
        }
    } else {
        cout << FAIL;
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
    //system("ls -laR ."); // Manually added

    gtfs_clean(gtfs);

    cout << "After GTFS cleanup\n";
    system("ls -l .");
    //system("ls -laR ."); // Manually added

    cout << "If log is truncated: " << PASS << "If exactly same output:" << FAIL;

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

    //Commenting this out to manually see if the logs is writing correctly
    gtfs_close_file(gtfs, fl);
    
    

    //Now test out the reading
    fl = gtfs_open_file(gtfs, filename, 100);

    char *data_0 = gtfs_read_file(gtfs, fl, 0, str.length());

    if (data_0 != NULL) {
        str.compare(string(data_0)) == 0 ? cout << PASS : cout << FAIL << " Data is: " << data_0 << "(END)\n";
    } else {
        cout << FAIL << " Data is null!\n";
    }

    char *data_1 = gtfs_read_file(gtfs, fl, 0, str.length());

    if (data_1 != NULL) {
        str.compare(string(data_1)) == 0 ? cout << PASS : cout << FAIL << " Data is: " << data_1 << "(END)\n";
    } else {
        cout << FAIL << " Data is null!\n";
    }

    gtfs_close_file(gtfs, fl);
    
}

// TODO: Implement any additional tests

int main(int argc, char **argv) {
    if (argc < 2)
        printf("Usage: ./test verbose_flag\n");
    else
        verbose = strtol(argv[1], NULL, 10);

    // Get current directory path
    char cwd[256];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        directory = string(cwd);
    } else {
        cout << "[cwd] Something went wrong.\n";
    }

    // Call existing tests
    cout << "================== Test 1 ==================\n";
    cout << "Testing that data written by one process is then successfully read by another process.\n";
    test_write_read();

    cout << "================== Test 2 ==================\n";
    cout << "Testing that aborting a write returns the file to its original contents.\n";
    test_abort_write();

    cout << "================== Test 3 ==================\n";
    cout << "Testing that the logs are truncated.\n";
    test_truncate_log();

    // TODO: Call any additional tests

    cout << "================== Test 4 ==================\n";
    cout << "Testing multiple writes\n";
    test_multiple_writes();

}
