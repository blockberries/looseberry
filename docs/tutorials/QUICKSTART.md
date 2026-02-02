# Quickstart Tutorial

Get up and running with Looseberry in 5 minutes.

## What You'll Build

A simple single-node Looseberry instance that:
- Accepts transactions
- Creates batches and certificates
- Maintains a DAG of certificates
- Demonstrates the complete workflow

## Prerequisites

- Go 1.21 or later installed
- 5 minutes of your time

## Step 1: Create Project

Create a new directory and initialize a Go module:

```bash
mkdir looseberry-quickstart
cd looseberry-quickstart
go mod init quickstart
```

## Step 2: Install Looseberry

Add Looseberry as a dependency:

```bash
go get github.com/blockberries/looseberry
```

## Step 3: Write the Code

Create `main.go`:

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/blockberries/looseberry"
	"github.com/blockberries/looseberry/network"
	"github.com/blockberries/looseberry/types"
)

func main() {
	fmt.Println("=== Looseberry Quickstart ===")

	// Step 1: Generate validator key
	fmt.Println("\n1. Generating validator key...")
	signer, err := types.GenerateEd25519Signer(0)
	if err != nil {
		log.Fatalf("Failed to generate signer: %v", err)
	}
	fmt.Printf("   ✓ Generated key for validator 0\n")

	// Step 2: Create validator set
	fmt.Println("\n2. Creating validator set...")
	validators := []*types.Validator{
		{Index: 0, PublicKey: signer.PublicKey()},
	}
	validatorSet := types.NewSimpleValidatorSet(validators, 0)
	fmt.Printf("   ✓ Validator set created (1 validator)\n")

	// Step 3: Configure Looseberry
	fmt.Println("\n3. Configuring Looseberry...")
	cfg := looseberry.DefaultConfig()
	cfg.ValidatorIndex = 0
	cfg.Signer = signer
	cfg.Storage.InMemory = true // Use in-memory storage

	// Fast configuration for demo
	cfg.Worker.BatchSize = 10
	cfg.Worker.BatchTimeout = 500 * time.Millisecond
	cfg.Primary.HeaderTimeout = 1 * time.Second

	fmt.Printf("   ✓ Configuration created\n")

	// Step 4: Create Looseberry instance
	fmt.Println("\n4. Creating Looseberry instance...")
	lb, err := looseberry.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create Looseberry: %v", err)
	}
	fmt.Printf("   ✓ Looseberry created\n")

	// Step 5: Set validator set and network
	fmt.Println("\n5. Setting up dependencies...")
	lb.SetValidatorSet(validatorSet)
	mockNet := network.NewMockNetwork(0, network.DefaultConfig())
	lb.SetNetwork(mockNet)
	fmt.Printf("   ✓ Dependencies configured\n")

	// Step 6: Start Looseberry
	fmt.Println("\n6. Starting Looseberry...")
	if err := lb.Start(); err != nil {
		log.Fatalf("Failed to start Looseberry: %v", err)
	}
	defer lb.Stop()
	fmt.Printf("   ✓ Looseberry started successfully!\n")

	// Step 7: Submit transactions
	fmt.Println("\n7. Submitting transactions...")
	for i := 0; i < 25; i++ {
		tx := []byte(fmt.Sprintf("Transaction #%d", i+1))
		if err := lb.AddTx(tx); err != nil {
			log.Printf("   ⚠ Failed to add transaction %d: %v", i+1, err)
		}
	}
	fmt.Printf("   ✓ Submitted 25 transactions\n")

	// Step 8: Wait for processing
	fmt.Println("\n8. Waiting for batches and certificates...")
	time.Sleep(3 * time.Second)

	// Step 9: Check status
	fmt.Println("\n9. Checking status...")
	metrics := lb.Metrics()
	fmt.Printf("   Current round: %d\n", metrics.CurrentRound)
	fmt.Printf("   Total batches: %d\n", metrics.TotalBatches)
	fmt.Printf("   Total certificates: %d\n", metrics.TotalCertificates)
	fmt.Printf("   Pending transactions: %d\n", metrics.PendingTxCount)
	fmt.Printf("   Worker count: %d\n", metrics.WorkerCount)

	// Step 10: Reap certified batches
	fmt.Println("\n10. Reaping certified batches...")
	certifiedBatches := lb.ReapCertifiedBatches(1024 * 1024) // 1MB
	fmt.Printf("   ✓ Reaped %d certified batches\n", len(certifiedBatches))

	// Show batch details
	if len(certifiedBatches) > 0 {
		fmt.Println("\n   Batch details:")
		for i, cb := range certifiedBatches {
			fmt.Printf("     Batch %d:\n", i+1)
			fmt.Printf("       Round: %d\n", cb.Certificate.Header.Round)
			fmt.Printf("       Validator: %d\n", cb.Certificate.Header.Author)
			fmt.Printf("       Transactions: %d\n", len(cb.Batch.Transactions))
		}
	}

	// Step 11: Simulate consensus commit
	if len(certifiedBatches) > 0 {
		fmt.Println("\n11. Simulating consensus commit...")
		highestRound := uint64(0)
		for _, cb := range certifiedBatches {
			if cb.Certificate.Header.Round > highestRound {
				highestRound = cb.Certificate.Header.Round
			}
		}
		lb.NotifyCommitted(highestRound)
		fmt.Printf("   ✓ Committed up to round %d\n", highestRound)

		// Check metrics after commit
		time.Sleep(100 * time.Millisecond)
		metrics = lb.Metrics()
		fmt.Printf("   Committed round: %d\n", metrics.CommittedRound)
	}

	fmt.Println("\n=== Quickstart Complete! ===")
	fmt.Println("\nYou've successfully:")
	fmt.Println("  ✓ Created a Looseberry instance")
	fmt.Println("  ✓ Submitted transactions")
	fmt.Println("  ✓ Formed batches and certificates")
	fmt.Println("  ✓ Reaped certified batches")
	fmt.Println("  ✓ Simulated consensus commit")
	fmt.Println("\nNext steps:")
	fmt.Println("  - Read the Getting Started guide")
	fmt.Println("  - Try the Multi-Node tutorial")
	fmt.Println("  - Explore the Configuration guide")
}
```

## Step 4: Run It!

```bash
go mod tidy
go run main.go
```

Expected output:

```
=== Looseberry Quickstart ===

1. Generating validator key...
   ✓ Generated key for validator 0

2. Creating validator set...
   ✓ Validator set created (1 validator)

3. Configuring Looseberry...
   ✓ Configuration created

4. Creating Looseberry instance...
   ✓ Looseberry created

5. Setting up dependencies...
   ✓ Dependencies configured

6. Starting Looseberry...
   ✓ Looseberry started successfully!

7. Submitting transactions...
   ✓ Submitted 25 transactions

8. Waiting for batches and certificates...

9. Checking status...
   Current round: 3
   Total batches: 3
   Total certificates: 3
   Pending transactions: 0
   Worker count: 1

10. Reaping certified batches...
   ✓ Reaped 3 certified batches

   Batch details:
     Batch 1:
       Round: 1
       Validator: 0
       Transactions: 10
     Batch 2:
       Round: 2
       Validator: 0
       Transactions: 10
     Batch 3:
       Round: 3
       Validator: 0
       Transactions: 5

11. Simulating consensus commit...
   ✓ Committed up to round 3
   Committed round: 3

=== Quickstart Complete! ===

You've successfully:
  ✓ Created a Looseberry instance
  ✓ Submitted transactions
  ✓ Formed batches and certificates
  ✓ Reaped certified batches
  ✓ Simulated consensus commit

Next steps:
  - Read the Getting Started guide
  - Try the Multi-Node tutorial
  - Explore the Configuration guide
```

## What Just Happened?

Let's break down what happened:

### 1. Key Generation
```go
signer, _ := types.GenerateEd25519Signer(0)
```
Generated an Ed25519 key pair for validator 0.

### 2. Validator Set
```go
validators := []*types.Validator{
    {Index: 0, PublicKey: signer.PublicKey()},
}
validatorSet := types.NewSimpleValidatorSet(validators, 0)
```
Created a validator set with a single validator.

### 3. Configuration
```go
cfg := looseberry.DefaultConfig()
cfg.Storage.InMemory = true
```
Used default configuration with in-memory storage.

### 4. Transaction Flow
```
Submit Tx → Worker Batching → Header Creation →
Certificate Formation → DAG → Consensus Reaps
```

### 5. Certificate Formation
With a single validator:
- No voting needed (quorum = 1)
- Certificates form immediately
- DAG advances automatically

### 6. Reaping
```go
certifiedBatches := lb.ReapCertifiedBatches(maxBytes)
```
Consensus retrieves certified batches for block building.

### 7. Commit Notification
```go
lb.NotifyCommitted(round)
```
Notifies Looseberry that consensus committed a round.

## Understanding the Output

**Round progression**: Rounds advance as certificates are formed
- Round 1: First 10 transactions → Certificate 1
- Round 2: Next 10 transactions → Certificate 2
- Round 3: Last 5 transactions → Certificate 3

**Batch creation**: Triggered by size (10 txs) or timeout (500ms)

**Certificate formation**: Immediate with single validator

## Experiment!

Try modifying the code:

### More Transactions
```go
for i := 0; i < 100; i++ {
    tx := []byte(fmt.Sprintf("Transaction #%d", i+1))
    lb.AddTx(tx)
}
```

### Smaller Batches
```go
cfg.Worker.BatchSize = 5  // Smaller batches
```

### Faster Rounds
```go
cfg.Primary.HeaderTimeout = 500 * time.Millisecond  // Faster headers
```

### Monitor Metrics
```go
ticker := time.NewTicker(1 * time.Second)
for i := 0; i < 10; i++ {
    <-ticker.C
    metrics := lb.Metrics()
    fmt.Printf("Round: %d, Pending: %d\n",
        metrics.CurrentRound, metrics.PendingTxCount)
}
```

## Troubleshooting

**Issue**: Nothing happens after submitting transactions

**Solution**: Wait longer or reduce timeouts:
```go
cfg.Worker.BatchTimeout = 100 * time.Millisecond
cfg.Primary.HeaderTimeout = 200 * time.Millisecond
time.Sleep(5 * time.Second) // Wait longer
```

**Issue**: "validator set not set" error

**Solution**: Ensure you call `SetValidatorSet()` before `Start()`:
```go
lb.SetValidatorSet(validatorSet)
lb.Start()
```

## Next Steps

Now that you've completed the quickstart, explore:

1. **[Getting Started Guide](../guides/GETTING_STARTED.md)**: Deeper dive into Looseberry
2. **[Multi-Node Tutorial](MULTI_NODE.md)**: Set up a multi-node network
3. **[Integration Guide](../guides/INTEGRATION.md)**: Integrate with consensus
4. **[Configuration Guide](../guides/CONFIGURATION.md)**: Tune configuration

## Complete Code

The complete code is available at:
`docs/tutorials/quickstart/main.go`

You can run it directly:

```bash
git clone https://github.com/blockberries/looseberry
cd looseberry/docs/tutorials/quickstart
go run main.go
```

Congratulations! You've completed the Looseberry quickstart tutorial.
