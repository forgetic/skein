use skein_macros::scope;

fn main() {
    // scope! requires a body block
    scope!(cx, "name");
}
