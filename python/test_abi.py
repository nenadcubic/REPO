import ctypes as C
from ctypes import c_char_p, c_int, c_size_t, c_uint16, POINTER

lib = C.CDLL("./build/liber_abi.so")

lib.er_create.restype = C.c_void_p
lib.er_create.argtypes = [c_char_p, c_int]

lib.er_destroy.argtypes = [C.c_void_p]
lib.er_ping.argtypes = [C.c_void_p]
lib.er_ping.restype = c_int

lib.er_put_bits.argtypes = [C.c_void_p, c_char_p, POINTER(c_uint16), c_size_t]
lib.er_put_bits.restype = c_int

lib.er_find_all_store.argtypes = [
    C.c_void_p, c_int, POINTER(c_uint16), c_size_t, c_char_p, c_size_t
]
lib.er_find_all_store.restype = c_int

lib.er_show_set.argtypes = [C.c_void_p, c_char_p, c_char_p, c_size_t]
lib.er_show_set.restype = c_int

h = lib.er_create(b"redis", 6379)
assert h
assert lib.er_ping(h) == 0

bits = (c_uint16 * 2)(42, 7)
assert lib.er_put_bits(h, b"a", bits, 2) == 0

bits2 = (c_uint16 * 2)(42, 7)
tmp = C.create_string_buffer(256)
assert lib.er_find_all_store(h, 10, bits2, 2, tmp, len(tmp)) == 0

out = C.create_string_buffer(1024)
assert lib.er_show_set(h, tmp.value, out, len(out)) == 0
print("RESULTS:\n", out.value.decode())

lib.er_destroy(h)

