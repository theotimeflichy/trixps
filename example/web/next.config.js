/** @type {import('next').NextConfig} */
const nextConfig = {
  // build a slim self-contained server for the docker image
  output: "standalone",
};

module.exports = nextConfig;
