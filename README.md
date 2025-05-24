# Wholesum network `Prover` CLI

## Overview

Wholesum network is a p2p prover network `tailored for ETH L1 block proving`. It builds on top of [Risc0](https://risczero.com/) and [Libp2p](https://libp2p.io). The design of the network follows a p2p parallel proving scheme where Risc0 jobs are passed around, proved, and aggregated into a final Groth16 proof ready for L1 verification.

## How to run

Bringing a prover node up involves few steps.

### Prerequisites

You would need to get certain environments ready for the prover node to function properly.

#### Risc0 

To install Risc0, please follow the following [guide](https://github.com/risc0/risc0?tab=readme-ov-file#getting-started).

#### Docker

Docker runtime is needed as it is used to run `Risc0` containers. This awesome [guide](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04) from DigitalOcean is helpful in this regard.

### Library dependencies

To run a prover node, you would first need to fork the following libraries and put them in the parent("..") directory of the project:

- [comms](https://github.com/WholesumNet/comms)

### USAGE

<pre>
Wholesum is a p2p prover network and this program is a CLI for prover nodes.

Usage: prover [OPTIONS]

Options:
      --dev                  
  -k, --key-file <KEY_FILE>  
  -h, --help                 Print help
  -V, --version              Print version
</pre>
