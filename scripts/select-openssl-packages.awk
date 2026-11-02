# Select the owners of the libraries copied into the distroless runtime.
# libssl3/libcrypto3 are virtual packages in newer Chainguard repositories;
# their installed names and SBOM filenames can include an OpenSSL version.
BEGIN { RS = ""; FS = "\n"; ORS = "\n\n" }
{
    directory = ""
    selected = 0
    for (i = 1; i <= NF; i++) {
        if ($i ~ /^F:/) directory = substr($i, 3)
        if (directory == "usr/lib" && $i == "R:libssl.so.3") {
            ssl++
            selected = 1
        }
        if (directory == "usr/lib" && $i == "R:libcrypto.so.3") {
            crypto++
            selected = 1
        }
    }
    if (selected) print $0
}
END {
    if (ssl != 1 || crypto != 1) {
        print "Expected exactly one installed owner for each OpenSSL 3 runtime library" > "/dev/stderr"
        exit 1
    }
}
