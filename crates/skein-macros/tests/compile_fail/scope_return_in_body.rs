use skein_macros::scope;

async fn example(cx: &()) {
    // return is forbidden inside scope! body
    scope!(cx, { return 42; });
}

fn main() {}
