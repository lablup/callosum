service SimpleService {
    string echo(1:string msg),
    i64 add(1:i64 a, 2:i64 b),
    bool oops(),
    bool long_delay(),
}
