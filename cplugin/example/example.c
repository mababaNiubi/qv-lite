// example.c — demonstrate the qv-lite C API with compact qv_value_t.
//
// Build (from the cplugin directory):
//   make test
//
// Or manually:
//   gcc -o build/example example.c -Ibuild -Lbuild -lqv_lite -lpthread -ldl
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "qv_lite.h"

// Print a qv_value_t using the accessor helpers.
static void print_value(qv_value_t v) {
    switch (v.value_type) {
    case QV_TYPE_EMPTY:   printf("empty");                                break;
    case QV_TYPE_INT64:   printf("int64=%lld", (long long)qv_as_int64(v));   break;
    case QV_TYPE_UINT64:  printf("uint64=%llu", (unsigned long long)qv_as_uint64(v)); break;
    case QV_TYPE_FLOAT64: printf("float64=%.4f", qv_as_float64(v));           break;
    case QV_TYPE_BOOL:    printf("bool=%d",    qv_as_bool(v));               break;
    case QV_TYPE_STRING:  printf("string=\"%s\"", qv_as_string(v));           break;
    case QV_TYPE_JSON:    printf("json=\"%s\"",   qv_as_string(v));           break;
    default:              printf("unknown");                                break;
    }
}

int main(void) {
    char* ver = qv_version();
    printf("qv-lite version: %s\n", ver);
    qv_free_string(ver);

    // ---- Open ----
    qv_config_t cfg = {0};
    cfg.path                         = "./example-data";
    cfg.max_file_size                = 64 * 1024 * 1024;
    cfg.max_segment_size             = 64 * 1024 * 1024;
    cfg.max_buffer_batch_size        = 4096;
    cfg.max_storage_time             = 3600;
    cfg.secondary_compression_name   = "zstd";

    qv_handle db = qv_open(cfg);
    if (db == 0) { fprintf(stderr, "qv_open failed\n"); return 1; }
    printf("Database opened.\n");

    // ---- Single writes (one function, all types) ----
    int64_t now = (int64_t)time(NULL) * 1000000000LL;
    char* err;

    err = qv_write(db, "", "sensor_a", now,              qv_int64(42));
    if (err) { fprintf(stderr, "write: %s\n", err); qv_free_string(err); }

    err = qv_write(db, "", "sensor_a", now + 1000000000LL, qv_float64(3.14));
    if (err) { fprintf(stderr, "write: %s\n", err); qv_free_string(err); }

    err = qv_write(db, "", "sensor_b", now,                qv_string("hello"));
    if (err) { fprintf(stderr, "write: %s\n", err); qv_free_string(err); }

    err = qv_write(db, "", "sensor_b", now + 1000000000LL, qv_bool(1));
    if (err) { fprintf(stderr, "write: %s\n", err); qv_free_string(err); }

    err = qv_write(db, "", "sensor_c", now,
                   qv_json("{\"temp\":23.5,\"humidity\":60,\"status\":\"ok\"}"));
    if (err) { fprintf(stderr, "write: %s\n", err); qv_free_string(err); }

    // Write a list (no JSON parsing — direct variant construction)
    qv_value_t list_items[] = {qv_int64(1), qv_float64(2.5), qv_string("three")};
    err = qv_write(db, "", "sensor_d", now, qv_list(list_items, 3));
    if (err) { fprintf(stderr, "write list: %s\n", err); qv_free_string(err); }

    // Write a map (no JSON parsing — direct variant construction)
    qv_kv_t map_pairs[] = {
        {.key = "name",  .value = qv_string("device_1")},
        {.key = "value", .value = qv_float64(99.9)},
        {.key = "tags",  .value = qv_string("a,b,c")},
    };
    err = qv_write(db, "", "sensor_e", now, qv_map(map_pairs, 3));
    if (err) { fprintf(stderr, "write map: %s\n", err); qv_free_string(err); }

    // Nested structure: a map containing a list
    qv_value_t inner_items[] = {qv_int64(10), qv_int64(20), qv_int64(30)};
    qv_kv_t nested_pairs[] = {
        {.key = "id",    .value = qv_int64(1)},
        {.key = "readings", .value = qv_list(inner_items, 3)},
    };
    err = qv_write(db, "", "sensor_f", now, qv_map(nested_pairs, 2));
    if (err) { fprintf(stderr, "write nested: %s\n", err); qv_free_string(err); }

    printf("Wrote 8 single points (incl. list, map, nested).\n");

    // ---- Batch write (mixed types) ----
    qv_batch_point_t batch[] = {
        {.tag = "sensor_a", .timestamp = now + 2000000000LL, .value = qv_int64(100)},
        {.tag = "sensor_a", .timestamp = now + 3000000000LL, .value = qv_float64(2.718)},
        {.tag = "sensor_b", .timestamp = now + 4000000000LL, .value = qv_string("batch_hello")},
        {.tag = "sensor_b", .timestamp = now + 5000000000LL, .value = qv_bool(0)},
        {.tag = "sensor_c", .timestamp = now + 6000000000LL, .value = qv_json("{\"x\":1,\"y\":[2,3,4]}")},
        {.tag = "sensor_d", .timestamp = now + 7000000000LL,
         .value = qv_list((qv_value_t[]){qv_int64(7), qv_int64(8), qv_int64(9)}, 3)},
    };
    err = qv_write_batch(db, "", batch, 6);
    if (err) { fprintf(stderr, "batch write: %s\n", err); qv_free_string(err); }
    else     { printf("Batch-wrote 6 points.\n"); }

    // ---- Query ----
    qv_result_t* r = qv_query_all(db, "", "sensor_a", 0, now + 10000000000LL);
    if (r->error) {
        fprintf(stderr, "query error: %s\n", r->error);
    } else {
        printf("Query returned %d points for sensor_a:\n", r->count);
        for (int i = 0; i < r->count; i++) {
            printf("  t=%lld  ", (long long)r->points[i].timestamp);
            print_value(r->points[i].value);
            printf("\n");
        }
    }
    qv_free_result(r);

    // ---- Query JSON ----
    r = qv_query_all(db, "", "sensor_c", 0, now + 10000000000LL);
    if (r->error) {
        fprintf(stderr, "query error: %s\n", r->error);
    } else {
        printf("Query returned %d points for sensor_c (JSON):\n", r->count);
        for (int i = 0; i < r->count; i++) {
            printf("  t=%lld  ", (long long)r->points[i].timestamp);
            print_value(r->points[i].value);
            printf("\n");
        }
    }
    qv_free_result(r);

    // ---- Query structured data (list / map / nested) ----
    const char* structured_tags[] = {"sensor_d", "sensor_e", "sensor_f"};
    for (int j = 0; j < 3; j++) {
        r = qv_query_all(db, "", structured_tags[j], 0, now + 10000000000LL);
        if (r->error) {
            fprintf(stderr, "query %s error: %s\n", structured_tags[j], r->error);
        } else if (r->count > 0) {
            printf("%s: t=%lld  ", structured_tags[j], (long long)r->points[0].timestamp);
            print_value(r->points[0].value);
            printf("\n");
        }
        qv_free_result(r);
    }

    // ---- Latest ----
    r = qv_query_latest(db, "", "sensor_b");
    if (r->error) {
        fprintf(stderr, "latest error: %s\n", r->error);
    } else if (r->count > 0) {
        printf("Latest sensor_b: t=%lld value=", (long long)r->points[0].timestamp);
        print_value(r->points[0].value);
        printf("\n");
    }
    qv_free_result(r);

    // ----
    qv_close(db);
    printf("Done.\n");
    return 0;
}
