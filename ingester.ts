import {Connection, PublicKey} from '@solana/web3.js';
import yargs from "yargs";
import {hideBin} from "yargs/helpers";
import * as fs from "fs";
import * as readline from "readline";
import { open, stat, appendFile } from 'node:fs/promises';

require('dotenv').config();

export enum Mode {
  History = "History",
  Standard = "Standard",
}

/*
  NOTES

  To keep track of progress I chose to read most recent state written to a file.
  It is done only once when the program starts (and if an exception is thrown).
  As file grows it will become less efficient, however there are ways to optimized this.
  For example, the file can be read from the bottom or a separate state can be maintianed.
  I would also expect that in a real-life implmentation a database would be used, and
  finding the most recent signature would take little time.

  In recards to part C, I am leaning:
  1. Towards saving a signature even if it is returned by only one provider, but I may be missing
    something here.
  2. To make sure we don't miss any signatures I chose to only save signatures that are returned
    by no fewer than 2/3 of the providers and next time fetching from the last signature saved.
    There's a possible infinite loop there if we continue to get that same signature from one
    provider and not the others. This can be handled by counting number of times we saw this
    case and moving on after enough attempts have been made (I did not implement this approach
    here). However, this case would be indicative of a problem elsewhere, likely with providers.
    In reality I would expect such signature either to eventually be returned by other providers
    or disappear altogether. Although, this could thake time, so in production I'd add a safe
    guard as mentioned above.
*/


(async () => {
  const args = await yargs(hideBin(process.argv))
    .command("ingester", "ingests Solana onchain sigantures from a given marketplace address")
    .option("mode", {
      alias: "m",
      describe: "mode: History or Standard",
      type: "string",
      default: Mode.Standard,
      choices: Object.values(Mode),
    })
    .option("marketplace", {
      alias: "mp",
      describe: "marketplace to fetch sigs for",
      type: "string",
      default: "TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN"
    })
    .option("sigFetchSize", {
      alias: "sf",
      describe: "# of sigs to fetch at a time (max 1000)",
      type: "number",
      default: 1000,
    })
    .option("sigWriteSize", {
      alias: "sw",
      describe: "# of sigs to write at a time (max 50)",
      type: "number",
      default: 50,
    })
    .option("part", {
      alias: "pt",
      describe: "Part name (A, B, or C)",
      type: "string",
      default: 'A',
      choices: ['A', 'B', 'C'],
    })
    .strict().argv;

  console.log('Ingester reporting to duty 🫡')

  const rpc = process.env.RPC_PROVIDER;
  if (!rpc) {
    console.log('Ooof missing RPC. Did you add one to .env?')
    return
  };

  if (args.part === 'A') {
    await partA(rpc, args, 'OUTPUT_A.csv');
  } else if (args.part === 'B') {
    await partB(rpc, args, 'OUTPUT_B.csv');
  } else if (args.part === 'C') {
    await partC([rpc, rpc, rpc], args, 'OUTPUT_C.csv');
  }

  /*
  const connection = new Connection(rpc);
  const marketplaceAddress = new PublicKey(args.marketplace);

  //hm I wonder how args.mode interacts with this? 🤔
  const sigs = await connection.getSignaturesForAddress(marketplaceAddress, {limit: args.sigFetchSize})

  console.log(`Looks like I fetched ${sigs.length} sigs.`)
  console.log(`Here's what one of them looks like: ${JSON.stringify(sigs[0], null, 4)}`)

  console.log('Let me write a few of them down into OUTPUT.txt as an example')
  fs.appendFileSync('OUTPUT.txt', sigs.slice(0, args.sigWriteSize).map(s => `${s.blockTime}:${s.signature}`).join('\r\n'))
*/
})()

const HEADER = 'sig,blockTime'

interface BlockInfo {
  blockTime: number;
  signature: string;
}

const isStandardMode = (args: any): boolean => args.mode === Mode.Standard

async function getLastProcessedSignature(fileName: string): Promise<BlockInfo | null> {
  var lastLine = undefined;
  try {
    const file = await open(fileName); // note: this will fail if file doesn't exist
    let headerRead = false;
    for await (const line of file.readLines()) {
      if (headerRead) {
        lastLine = line;
      } else {
        headerRead = line.startsWith(HEADER);
      }
    }
  } catch (error) {
    // log something here if desired; maybe, handle in some other way
  }
  const lastLineElements = lastLine?.split(',');
  if (lastLineElements) {
    return {
      blockTime: parseInt(lastLineElements[1]),
      signature: lastLineElements[0]
    }
  } else {
    return null
  }
}

// Find oldest or newest last processed time
// It relies on a fact that we are writing data into a file as we get it,
// newest to oldest, and so the first time stamp we encountere for a block
// is for the most recent signature
// It would be possible to use a library that reads CSV files, but I didn't want
// to include more packages unless bsolutely necessary
async function findProcessedSignature(fileName: string, newest: boolean): Promise<BlockInfo | null> {
  var blockTime = undefined;
  var signature = undefined;
  try {
    const file = await open(fileName); // note: this will fail if file doesn't exist
    let headerRead = false
    // I am not sure whether "readLines" reads the entire file into memory. If it does
    // it would be better to use something that reads one line at a time to reduce memory
    // consumotion
    for await (const line of file.readLines()) {
      if (headerRead) {
        const lineElements = line.split(',')
        const newBlockTime = parseInt(lineElements[1])
        if (!blockTime || (newest && newBlockTime > blockTime) || (!newest && newBlockTime <= blockTime)) {
          blockTime = newBlockTime
          signature = lineElements[0]
        }
      } else {
        headerRead = line.startsWith(HEADER)
      }
    }
  } catch (error) {
    // log something here if desired; maybe, handle in some other way
  }
  if (blockTime && signature) {
    return {
      blockTime,
      signature
    }
  } else {
    return null
  }
}

async function readSigs(rpc: string, marketplace: string, limit: number, before?: string, until?: string): Promise<any[]> {
  const connection = new Connection(rpc)
  const marketplaceAddress = new PublicKey(marketplace)

  return await connection.getSignaturesForAddress(marketplaceAddress,
    {
      limit, before, until
    }
  )
}

async function readSigsOptimized(connection: Connection, marketplaceAddress: PublicKey, limit: number, before?: string, until?: string): Promise<any[]> {
  return await connection.getSignaturesForAddress(marketplaceAddress,
    {
      limit, before, until
    }
  )
}

async function appendSigsToFile(fileName: string, sigs: any[], args: any): Promise<void> {
  try {
    await stat(fileName)
  } catch (err) {
    await appendFile(fileName, `${HEADER}\r\n`)
  }
  for (let sliceIndex = 0; sliceIndex < sigs.length; sliceIndex += args.sigWriteSize) {
    await appendFile(
      fileName,
      sigs.slice(sliceIndex, sliceIndex + args.sigWriteSize)
      .map(s => `${s.signature},${s.blockTime}`).join('\r\n') + '\r\n'
    )
  }
}

// Filter, sort signatures and find most recent sognature
function massageSignatures(sigs: any[], lastProcessedSig: BlockInfo | null, args: any, toSort: boolean):
    {nextLastProcessedSig: string, nextLastProcessedTime: number, data: any[]} {
  const lastSig = isStandardMode(args) ? sigs[0] : sigs[sigs.length - 1]
  const nextLastProcessedSig = lastSig.signature
  const nextLastProcessedTime = lastSig.blockTime
  const timeFilter = (blockTime: number) =>
        isStandardMode(args)
          ? blockTime >= lastProcessedSig!!.blockTime
          : blockTime <= lastProcessedSig!!.blockTime

  // user a hash map to filter out duplicate signatues by using signatures as keys
  // keeps the last signature occurrence for each signature
  const data = Object.values(
    sigs
    // filter out signatures that are newer than the one from which we are searching
    // I assume that these got here by mistake
    // I was also considering using "slot" and stop processing if there's a gap, but
    // wasn't sure this is the right thing to do
    // Also, perhaps only keep signatures with "confirmationStatus" equal to "finalized"?
    // Note: this filter could be skipped if lastProcessedSig is null, but I went with this
    // implementation to keep it cleaner
    .filter((s) => !lastProcessedSig || timeFilter(s.blockTime))
    .reduce((map: any, s: any) => {
      map[s.signature] = s
      return map
      }, {}
    )
  )

  if (toSort) {
    if (isStandardMode(args)) {
      // default return order is new to old, we need to resort
      // another (faster, but slightly more convoluted) )option would be to move bottom up
      data.sort((a: any, b: any) => a.blockTime - b.blockTime)
    } else {
      // default return order is new to old, so we already have what we needed
      // but using has map may have broken the order, so need to sort again
      data.sort((a: any, b: any) => b.blockTime - a.blockTime)
    }
  }
  return {nextLastProcessedSig, nextLastProcessedTime, data}
}

async function partA(rpc: string, args: any, fileName: string): Promise<void> {
  let writerPromise = null
  let lastProcessedSig = await getLastProcessedSignature(fileName)
  const connection = new Connection(rpc)
  const marketplaceAddress = new PublicKey(args.marketplace)

  while (true) {
    try {
      const before = isStandardMode(args) ? undefined : lastProcessedSig?.signature
      const until = isStandardMode(args) ? lastProcessedSig?.signature : undefined
      // this call can be optimized by maintaining open "connection" and value of "marketplaceAddress"
      // but I kept its provided implementation fir simplicity's sake
      const sigs = await readSigsOptimized(connection, marketplaceAddress, args.sigFetchSize, before, until);

      console.log(`Fetched ${sigs.length} sigs.`)

      if (sigs.length === 0) {
        // try again
        continue
      }

      const { nextLastProcessedSig, nextLastProcessedTime, data } =
          massageSignatures(sigs, lastProcessedSig, args, true)

      console.log(`Will write ${data.length} sigs.`)

      if (writerPromise) {
        await writerPromise
      }

      lastProcessedSig = { signature: nextLastProcessedSig, blockTime: nextLastProcessedTime }

      writerPromise = appendSigsToFile(fileName, data, args);
    } catch (error) {
      console.log(error)
      // re-read last processed signature to make sure we are up to date
      lastProcessedSig = await getLastProcessedSignature(fileName)
    }
  }
}

async function partB(rpc: string, args: any, fileName: string): Promise<void> {
  let writerPromise = null
  let lastProcessedSig = await findProcessedSignature(fileName, isStandardMode(args))
  const connection = new Connection(rpc)
  const marketplaceAddress = new PublicKey(args.marketplace)

  while (true) {
    try {
      const before = isStandardMode(args) ? undefined : lastProcessedSig?.signature
      const until = isStandardMode(args) ? lastProcessedSig?.signature : undefined
      const sigs = await readSigsOptimized(connection, marketplaceAddress, args.sigFetchSize, before, until)

      console.log(`Fetched ${sigs.length} sigs.`)

      if (sigs.length === 0) {
        // try again
        continue
      }

      const { nextLastProcessedSig, nextLastProcessedTime, data } =
          massageSignatures(sigs, lastProcessedSig, args, false)

      console.log(`Will write ${data.length} sigs.`)

      if (writerPromise) {
        await writerPromise;
      }

      lastProcessedSig = { signature: nextLastProcessedSig, blockTime: nextLastProcessedTime }

      writerPromise = appendSigsToFile(fileName, data, args);
    } catch (error) {
      console.log(error)
      // re-read last processed signature to make sure we are up to date
      lastProcessedSig = await findProcessedSignature(fileName, isStandardMode(args))
    }
  }
}

async function partC(rpcs: string[], args: any, fileName: string): Promise<void> {
  let writerPromise = null;
  let lastProcessedSig = await getLastProcessedSignature(fileName)

  while (true) {
    try {
      const before = isStandardMode(args) ? undefined : lastProcessedSig?.signature
      const until = isStandardMode(args) ? lastProcessedSig?.signature : undefined

      const allFetchedSigs = await Promise.all(rpcs
        .map(async (rpc) => await readSigs(rpc, args.marketplace, args.sigFetchSize, before, until)))

      // Take blocks that were returned by at least 2/3 of rpcs
      // (for 3 rpcs - 2)
      // Flattens all records into one array, reduces to a hash map with
      // one entry per blocks and a counter of occurrences,
      // then filters those that appear in at lest 2/3 of rpcs
      // and then returns blocks
      const minNumOfOccurrences = Math.floor(rpcs.length * 2 / 3)
      const countedSigs = Object.values(allFetchedSigs.flat()
        .reduce((map: any, s: any) => {
          if (s.signature in map) {
            map[s.signature].counter++;
          } else {
            map[s.signature] = { block: s, counter: 1 }
          }
          return map
        }, {})
      )
      // default return order is new to old - restore it
      countedSigs.sort((a: any, b: any) => b.block.blockTime - a.block.blockTime)

      console.log(`Fetched ${countedSigs.length} sigs.`)

      // I am going to save those blocks that appear at least 2/3 of the time up
      // to the first block that appears fewer times (could be 0 blocks if the very
      // first block appears less that 2/3 of times)
      const firstOneTimeOccurrence = countedSigs.findIndex((s: any) => s.counter < minNumOfOccurrences)

      const sigs = (firstOneTimeOccurrence < 0
        ? countedSigs
        : countedSigs.slice(0, firstOneTimeOccurrence))
        .map((s: any) => s.block)

      if (sigs.length === 0) {
        // try again
        continue
      }

      const { nextLastProcessedSig, nextLastProcessedTime, data } =
          massageSignatures(sigs, lastProcessedSig, args, true)

      console.log(`Will write ${data.length} sigs.`)
      if (writerPromise) {
        await writerPromise;
      }

      lastProcessedSig = { signature: nextLastProcessedSig, blockTime: nextLastProcessedTime }

      writerPromise = appendSigsToFile(fileName, data, args)
    } catch (error) {
      console.log(error)
      // re-read last processed signature to make sure we are up to date
      lastProcessedSig = await getLastProcessedSignature(fileName)
    }
  }
}
