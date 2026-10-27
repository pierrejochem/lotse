// Re-embed the web app whenever `npm run build` changes it.
fn main() {
    println!("cargo:rerun-if-changed=../../console/web/dist");
}
