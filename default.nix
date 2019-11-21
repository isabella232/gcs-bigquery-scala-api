{ pkgs ? import <nixpkgs> {}, ... }:

let
  jdk = pkgs.adoptopenjdk-hotspot-bin-11;
  sbt = pkgs.sbt.override { jre = jdk; };
in
  pkgs.stdenv.mkDerivation {
    name = "gcs-bigquery-scala-api";
    src = ./.;
    buildInputs = [ jdk sbt ];
  }
