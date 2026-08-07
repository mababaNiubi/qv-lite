// perf_test2.c — isolating cgo overhead from fsync cost.
//
// Build:
//   gcc -O2 -o build/perf_test2.exe example/perf_test2.c -Ibuild -Lbuild -lqv_lite -lpthread
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "qv_lite.h"

#ifdef _WIN32
#include <windows.h>
static double now_ms(void) {
    LARGE_INTEGER freq, count;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&count);
    return (double)count.QuadPart * 1000.0 / (double)freq.QuadPart;
}
#else
static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1000000.0;
}
#endif

int main(void) {
    system("cmd /c rmdir /s /q perf-test-data2 2>nul");

    qv_config_t cfg = {0};
    cfg.path                         = "./perf-test-data2";
    cfg.max_file_size                = 256 * 1024 * 1024LL;
    cfg.max_buffer_batch_size        = 10000;
    cfg.secondary_compression_name   = "zstd";

    qv_handle db = qv_open(cfg);
    if (db == 0) { fprintf(stderr, "open failed\n"); return 1; }
    int64_t ts = (int64_t)time(NULL) * 1000000000LL;

    // ── Phase 1: warm up — create 1000 tags via one batch ──
    printf("=== Phase 1: creating 1000 tags via batch ===\n");
    double t0 = now_ms();
    {
        qv_batch_point_t warm[1000];
        char tag[16];
        for (int i = 0; i < 1000; i++) {
            snprintf(tag, sizeof(tag), "tag_%04d", i);
            warm[i].tag       = tag;
            warm[i].timestamp = ts;
            warm[i].value     = qv_int64(0);
        }
        char* err = qv_write_batch(db, "", warm, 1000);
        double el = now_ms() - t0;
        printf("  1000 tag-creating writes in %.0f ms  (incl. 1000 fsyncs)\n", el);
        if (err) { qv_free_string(err); }
    }

    // ── Phase 2: single writes to existing tags (no fsync!) ──
    printf("\n=== Phase 2: 200K single writes (warm tags, no fsync) ===\n");
    t0 = now_ms();
    char tag[16];
    for (int i = 0; i < 200000; i++) {
        snprintf(tag, sizeof(tag), "tag_%04d", i % 1000);
        char* err = qv_write(db, "", tag, ts + (int64_t)i * 1000000LL, qv_int64(i));
        if (err) { qv_free_string(err); break; }
    }
    double el = now_ms() - t0;
    printf("  200000 writes in %.0f ms  →  %.0f pts/s  (no fsync)\n",
           el, 200000.0 / (el / 1000.0));

    // ── Phase 3: batch writes for comparison ──
    printf("\n=== Phase 3: 200K batch writes (same tags) ===\n");
    t0 = now_ms();
    {
        qv_batch_point_t b[1000];
        for (int off = 0; off < 200000; off += 1000) {
            for (int i = 0; i < 1000; i++) {
                snprintf(tag, sizeof(tag), "tag_%04d", i);
                b[i].tag       = tag;
                b[i].timestamp = ts + (int64_t)(off + i) * 2000000LL;
                b[i].value     = qv_int64(off + i);
            }
            char* err = qv_write_batch(db, "", b, 1000);
            if (err) { qv_free_string(err); break; }
        }
    }
    el = now_ms() - t0;
    printf("  200000 batch writes in %.0f ms  →  %.0f pts/s\n",
           el, 200000.0 / (el / 1000.0));

    // ── Phase 4: C buffer approach — call qv_write but bulk into own batch ──
    printf("\n=== Phase 4: 200K writes via manual C-side batching ===\n");
    t0 = now_ms();
    {
        qv_batch_point_t buf[1000];
        int bi = 0;
        for (int i = 0; i < 200000; i++) {
            snprintf(tag, sizeof(tag), "tag_%04d", i % 1000);
            buf[bi].tag       = tag;
            buf[bi].timestamp = ts + (int64_t)i * 3000000LL;
            buf[bi].value     = qv_int64(i);
            bi++;
            if (bi == 1000) {
                char* err = qv_write_batch(db, "", buf, bi);
                if (err) { qv_free_string(err); break; }
                bi = 0;
            }
        }
        if (bi > 0) {
            char* err = qv_write_batch(db, "", buf, bi);
            if (err) qv_free_string(err);
        }
    }
    el = now_ms() - t0;
    printf("  200000 batched writes in %.0f ms  →  %.0f pts/s\n",
           el, 200000.0 / (el / 1000.0));

    qv_close(db);
    system("cmd /c rmdir /s /q perf-test-data2 2>nul");
    printf("\nDone.\n");
    return 0;
}
