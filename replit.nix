{ pkgs }: {
  deps = [
    pkgs.openssl
    pkgs.sqlite
   pkgs.nimble
   pkgs.nim
    pkgs.gcc
  ];
}