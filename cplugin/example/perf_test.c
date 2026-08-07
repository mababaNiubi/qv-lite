// perf_test.c — large-scale write and query performance test.
//
// Build:
//   gcc -O2 -o build/perf_test example/perf_test.c -Ibuild -Lbuild -lqv_lite -lpthread
//
// Run:
//   ./build/perf_test
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>

#include "qv_lite.h"

// Simple wall-clock timer (milliseconds since epoch).
static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);  // or _WIN32: QueryPerformanceCounter
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1000000.0;
}

#ifdef _WIN32
#include <windows.h>
static double win_now_ms(void) {
    LARGE_INTEGER freq, count;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&count);
    return (double)count.QuadPart * 1000.0 / (double)freq.QuadPart;
}
#undef now_ms
#define now_ms win_now_ms
#endif

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static void fail(const char* msg, char* err) {
    if (err) {
        fprintf(stderr, "%s: %s\n", msg, err);
        qv_free_string(err);
    } else {
        fprintf(stderr, "%s\n", msg);
    }
}

// Generate a simple tag name from an index.
static void tag_name(int i, char* buf, size_t sz) {
    snprintf(buf, sz, "tag_%05d", i % 1000);
}

// ---------------------------------------------------------------------------
// Test 1: Single-point writes (100K points, 1000 tags)
// ---------------------------------------------------------------------------
static void test_single_writes(qv_handle db, int total_points, int64_t base_ts) {
    printf("\n=== Test 1: Single-point writes (%d points, %d tags) ===\n",
           total_points, 1000);

    char tag[32];
    int errors = 0;
    double t0 = now_ms();

    for (int i = 0; i < total_points; i++) {
        tag_name(i, tag, sizeof(tag));
        int64_t ts = base_ts + (int64_t)i * 1000000LL;
        char* err = qv_write(db, "", tag, ts, qv_float64((double)i * 1.5 + 0.1));
        if (err) { errors++; qv_free_string(err); if (errors > 10) break; }
    }

    double elapsed = now_ms() - t0;
    printf("  Wrote %d points in %.0f ms  (%.0f pts/s)  errors=%d\n",
           total_points, elapsed, total_points / (elapsed / 1000.0), errors);
}

// ---------------------------------------------------------------------------
// Test 2: Batch writes (1M points, 100 tags, batch_size = 1000)
// ---------------------------------------------------------------------------
static void test_batch_writes(qv_handle db, int total_points, int batch_size, int64_t base_ts) {
    printf("\n=== Test 2: Batch writes (%d points, batch=%d) ===\n",
           total_points, batch_size);

    qv_batch_point_t* batch = (qv_batch_point_t*)malloc(
        (size_t)batch_size * sizeof(qv_batch_point_t));
    if (!batch) { fprintf(stderr, "malloc failed\n"); return; }

    char tag[32];
    int errors = 0, written = 0;
    double t0 = now_ms();

    for (int offset = 0; offset < total_points; offset += batch_size) {
        int n = batch_size;
        if (offset + n > total_points) n = total_points - offset;

        for (int i = 0; i < n; i++) {
            tag_name(offset + i, tag, sizeof(tag));
            batch[i].tag       = tag;
            batch[i].timestamp = base_ts + (int64_t)(offset + i) * 1000000LL;
            // Cycle through types: int64, float64, bool, string
            switch ((offset + i) % 4) {
            case 0: batch[i].value = qv_int64(offset + i);           break;
            case 1: batch[i].value = qv_float64((offset+i)*3.14159); break;
            case 2: batch[i].value = qv_bool((offset + i) % 3 == 0); break;
            default: {
                char buf[32]; snprintf(buf, sizeof(buf), "val_%d", offset + i);
                batch[i].value = qv_string(buf);
                break;
            }
            }
        }

        char* err = qv_write_batch(db, "", batch, n);
        if (err) { errors++; qv_free_string(err); if (errors > 10) break; }
        written += n;
    }

    double elapsed = now_ms() - t0;
    printf("  Wrote %d points in %.0f ms  (%.0f pts/s)  errors=%d\n",
           written, elapsed, written / (elapsed / 1000.0), errors);
    free(batch);
}

// ---------------------------------------------------------------------------
// Test 3: Write structured data (list / map) in batch
// ---------------------------------------------------------------------------
static void test_structured_writes(qv_handle db, int count, int64_t base_ts) {
    printf("\n=== Test 3: Structured writes (%d maps with list) ===\n", count);

    double t0 = now_ms();
    int errors = 0;

    for (int i = 0; i < count; i++) {
        // Build a map containing: {id: i, values: [i*1, i*2, i*3], on: bool}
        qv_value_t values[3];
        for (int j = 0; j < 3; j++) {
            values[j] = qv_float64((double)i * (double)(j + 1) * 0.5);
        }
        qv_kv_t pairs[] = {
            {.key = "id",     .value = qv_int64(i)},
            {.key = "values", .value = qv_list(values, 3)},
            {.key = "on",     .value = qv_bool(i % 2 == 0)},
        };

        int64_t ts = base_ts + (int64_t)i * 1000000LL;
        char* err = qv_write(db, "", "struct_tag", ts, qv_map(pairs, 3));
        if (err) { errors++; qv_free_string(err); }
    }

    double elapsed = now_ms() - t0;
    printf("  Wrote %d structured points in %.0f ms  (%.0f pts/s)  errors=%d\n",
           count, elapsed, count / (elapsed / 1000.0), errors);
}

// ---------------------------------------------------------------------------
// Test 4: Query — range scan
// ---------------------------------------------------------------------------
static void test_query_range(qv_handle db, char* tag,
                             int64_t start, int64_t end) {
    printf("\n=== Test 4: Range query (%s, %.3fs span) ===\n",
           tag, (double)(end - start) / 1e9);

    double t0 = now_ms();
    qv_result_t* r = qv_query_all(db, "", tag, start, end);
    double elapsed = now_ms() - t0;

    if (r->error) {
        fprintf(stderr, "  query error: %s\n", r->error);
    } else {
        printf("  Returned %d points in %.1f ms\n", r->count, elapsed);
        if (r->count > 0) {
            printf("  First: t=%lld ", (long long)r->points[0].timestamp);
            switch (r->points[0].value.value_type) {
            case QV_TYPE_INT64:  printf("int64=%lld\n", (long long)qv_as_int64(r->points[0].value)); break;
            case QV_TYPE_FLOAT64:printf("float64=%.2f\n", qv_as_float64(r->points[0].value)); break;
            case QV_TYPE_STRING: printf("string=%s\n", qv_as_string(r->points[0].value)); break;
            case QV_TYPE_BOOL:   printf("bool=%d\n", qv_as_bool(r->points[0].value)); break;
            default:             printf("\n"); break;
            }
        }
    }
    qv_free_result(r);
}

// ---------------------------------------------------------------------------
// Test 5: Aggregated query
// ---------------------------------------------------------------------------
static void test_query_aggregated(qv_handle db, char* tag,
                                  int64_t start, int64_t end, int64_t max_n) {
    printf("\n=== Test 5: Aggregated query (%s, %d buckets) ===\n",
           tag, (int)max_n);

    double t0 = now_ms();
    qv_result_t* r = qv_query(db, "", tag, start, end, max_n, 0 /* avg */);
    double elapsed = now_ms() - t0;

    if (r->error) {
        fprintf(stderr, "  query error: %s\n", r->error);
    } else {
        printf("  Aggregated to %d points in %.1f ms\n", r->count, elapsed);
        for (int i = 0; i < r->count && i < 5; i++) {
            printf("    [%d] t=%lld ", i, (long long)r->points[i].timestamp);
            switch (r->points[i].value.value_type) {
            case QV_TYPE_INT64:  printf("int64=%lld\n", (long long)qv_as_int64(r->points[i].value)); break;
            case QV_TYPE_FLOAT64:printf("float64=%.4f\n", qv_as_float64(r->points[i].value)); break;
            default:             printf("\n"); break;
            }
        }
        if (r->count > 5) printf("    ... (%d more)\n", r->count - 5);
    }
    qv_free_result(r);
}

// ---------------------------------------------------------------------------
// Test 6: Read structured data back
// ---------------------------------------------------------------------------
static void test_read_structured(qv_handle db, int64_t start, int64_t end) {
    printf("\n=== Test 6: Read structured data back ===\n");

    double t0 = now_ms();
    qv_result_t* r = qv_query_all(db, "", "struct_tag", start, end);
    double elapsed = now_ms() - t0;

    if (r->error) {
        fprintf(stderr, "  query error: %s\n", r->error);
    } else {
        printf("  Returned %d structured points in %.1f ms\n", r->count, elapsed);
        if (r->count > 0) {
            printf("  Sample: t=%lld  %s\n",
                   (long long)r->points[0].timestamp,
                   qv_as_string(r->points[0].value));
        }
    }
    qv_free_result(r);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(void) {
    printf("qv-lite performance test\n");

    // Clean up data from any prior run.
    system("cmd /c rmdir /s /q perf-test-data 2>nul");

    // Open database.
    qv_config_t cfg = {0};
    cfg.path                         = "./perf-test-data";
    cfg.max_file_size                = 256 * 1024 * 1024LL;  // 256 MiB WAL
    cfg.max_segment_size             = 256 * 1024 * 1024LL;
    cfg.max_buffer_batch_size        = 10000;
    cfg.max_storage_time             = 86400;              // 24h
    cfg.secondary_compression_name   = "zstd";

    qv_handle db = qv_open(cfg);
    if (db == 0) { fprintf(stderr, "qv_open failed\n"); return 1; }

    int64_t base_ts = (int64_t)time(NULL) * 1000000000LL;

    // --- Run tests ---
    int64_t t0_base = base_ts;
    test_single_writes(db,   100000, base_ts);              //  t0 .. t0+100s
    base_ts += 2000000000000LL;
    test_batch_writes(db,   1000000, 1000, base_ts);        //  t0+2000s .. t0+3000s
    base_ts += 2000000000000LL;
    test_structured_writes(db, 50000, base_ts);             //  t0+4000s .. t0+4050s
    base_ts += 2000000000000LL;

    // Query covers all three write windows: t0 .. t0+7000s
    int64_t query_start = t0_base;
    int64_t query_end   = base_ts;

    printf("\nQuery window: %.0fs .. %.0fs\n",
           (double)(query_start - t0_base) / 1e9,
           (double)(query_end - t0_base) / 1e9);

    test_query_range(db, "tag_00042", query_start, query_end);
    test_query_aggregated(db, "tag_00042", query_start, query_end, 500);
    test_read_structured(db, query_start, query_end);

    // --- Cleanup ---
    qv_close(db);
    system("cmd /c rmdir /s /q perf-test-data 2>nul");
    printf("\nDone.\n");
    return 0;
}
