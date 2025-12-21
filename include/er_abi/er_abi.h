#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef _WIN32
  #ifdef ER_ABI_EXPORTS
    #define ER_ABI_API __declspec(dllexport)
  #else
    #define ER_ABI_API __declspec(dllimport)
  #endif
#else
  #define ER_ABI_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct er_handle er_handle_t;

typedef enum er_status {
  ER_OK = 0,
  ER_ERR = 1,
  ER_BADARG = 2,
  ER_RANGE = 3,
  ER_REDIS = 4,
  ER_NOMEM = 5
} er_status_t;

/* lifecycle */
ER_ABI_API er_handle_t* er_create(const char* host, int port);
ER_ABI_API void         er_destroy(er_handle_t* h);
ER_ABI_API int          er_ping(er_handle_t* h);

/* error */
ER_ABI_API const char*  er_last_error(er_handle_t* h);

/* element ops */
ER_ABI_API int er_put_bits(er_handle_t* h, const char* name,
                           const uint16_t* bits, size_t n_bits);

/* composite store (Lua, atomic) */
ER_ABI_API int er_find_all_store(er_handle_t* h, int ttl_sec,
                                 const uint16_t* bits, size_t n_bits,
                                 char* out_tmp_key, size_t key_cap);

/* read members of a set key */
ER_ABI_API int er_show_set(er_handle_t* h, const char* set_key,
                           char* out, size_t out_cap);

int er_find_any_store(er_handle_t* h, int ttl_seconds,
                      const uint16_t* bits, size_t n_bits,
                      char* out_tmp_key, size_t key_cap);

int er_find_not_store(er_handle_t* h, int ttl_seconds,
                      const uint16_t* bits, size_t n_bits,
                      char* out_tmp_key, size_t key_cap);


#ifdef __cplusplus
}
#endif

