
// Postgres epoch (2000-01-01T00:00:00Z) minus Unix epoch, in microseconds
const PG_EPOCH_OFFSET_US = 946_684_800_000_000n

export const pgTimeToDate = (time: bigint): Date => new Date(Number((time + PG_EPOCH_OFFSET_US) / 1000n))
export const dateToPgTime = (date: Date): bigint => (BigInt(date.getTime()) * 1000n - PG_EPOCH_OFFSET_US)

export const readCString = (buf: Buffer, offset: number): [string, number] => {
    const end = buf.indexOf(0, offset)
    const string = buf.toString("utf-8", offset, end)
    return [string, end + 1]
}

