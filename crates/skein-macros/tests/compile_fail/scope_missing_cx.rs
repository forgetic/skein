use skein_macros::scope;

fn main() {
    // scope! requires a cx argument
    scope!({ let x = 1; });
}
