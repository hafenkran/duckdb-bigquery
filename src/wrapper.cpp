extern "C" {
    extern void __start_linkarr_upb_AllExts();
}

void force_include_upb_symbols() {
    __start_linkarr_upb_AllExts();
}
