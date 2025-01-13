# Wholesum network `Prover` CLI

## Overview

Wholesum network is a p2p verifiable computing network `tailored for ETH L2 sequencer proving`. It builds on top of [Risc0](https://risczero.com/), [Libp2p](https://libp2p.io), and decentralized storage options like [Swarm](https://ethswarm.org) and Filecoin to facilitate verifiable computing at scale. The design of the network follows a p2p parallel proving scheme where Risc0 jobs are passed around, proved, and finally combined into a final proof ready for L1 verification.

## How to run

Bringing a prover node up involves few steps.

### Prerequisites

You would need to get certain environments ready for the prover node to function properly.

#### Risc0 

To install Risc0, please follow the following [guide](https://github.com/risc0/risc0?tab=readme-ov-file#getting-started).


#### Docker

Docker runtime is needed as it is used to run `Risc0` containers. This awesome [guide](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04) from DigitalOcean is helpful in this regard.

#### Decentralized storage

- Lighthouse:  
  You would need a subscription plan from [Lighthouse](https://docs.lighthouse.storage/lighthouse-1/quick-start). Please obtain an API key, put it into a file with the following look:

  <pre>
    apiKey = 'your key'
  </pre>
  
- Swarm:
  Still under develepment.
  

### Library dependencies

To run a prover node, you would first need to fork the following libraries and put them in the parent("..") directory of the project:

- [comms](https://github.com/WholesumNet/comms)
- [dStorage](https://github.com/WholesumNet/dStorage)
- [jocker](https://github.com/WholesumNet/jocker)

### USAGE

<pre>
Wholesum is a P2P verifiable computing marketplace and this program is a CLI for prover nodes.

Usage: prover [OPTIONS]

Options:
  -d, --dstorage-key-file &lt;DSTORAGE_KEY_FILE&gt;  
      --dev                                    
  -k, --key-file &lt;KEY_FILE&gt;                    
  -h, --help                                   Print help
  -V, --version                                Print version

</pre>
