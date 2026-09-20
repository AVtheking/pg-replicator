import { Data, Match, Result, Array } from "effect"
import { PgReplError } from "./errors"
import { pgTimeToDate, readCString } from "./utils"

export enum CopyDataCode {
    Keepalive = "k",
    XLogData = "w",
}

export interface Keepalive {
    serverWalEnd: bigint
    serverTime: Date
    replyRequested: boolean
}

export type CopyData = Data.TaggedEnum<{
    Keepalive: Keepalive
    XLogData: {
        serverWalStart: bigint
        serverWalEnd: bigint
        serverTime: bigint
        walData: Buffer
    }
}>

export const CopyData = Data.taggedEnum<CopyData>()

export type Column = Data.TaggedEnum<{
    Null: {}
    Toast: {}
    Text: { value: string }
    Binary: { value: Uint8Array }
}>

export const Column = Data.taggedEnum<Column>()

export type TupleData = {
    numberOfColumns: number
    columns: Column[]
}

export type RelationColumn = {
    flag: number
    name: string
    dataTypeOID: number
    dataTypeModifier: number
}

export type RelationData = {
    relationId: number
    namespace: string
    name: string
    replicaIdentity: number
    numberOfColumns: number
    relationColumns: RelationColumn[]
}

export type PgOutput = Data.TaggedEnum<{
    Keepalive: Keepalive
    Begin: {
        finalLSN: bigint
        commitTimestamp: bigint
        xid: number
    }
    Commit: {
        flags: number
        commitLSN: bigint
        endLSN: bigint
        commitTimestamp: Date
    }
    Relation: RelationData
    Insert: {
        relationId: number
        tupleData: TupleData
        rows?: Record<string, unknown>
    }
    Update: {
        xid?: number
        relationId: number
        oldTupleKind?: "K" | "O"
        oldTupleData?: TupleData
        newTupleData: TupleData
        oldRows?: Record<string, unknown>
        newRows?: Record<string, unknown>
    }
    Delete: {
        xid?: number
        relationId: number
        oldTupleKind?: "K" | "O"
        oldTupleData?: TupleData
        rows?: Record<string, unknown>
    }
    Unknown: {
        type: string
        walData: Buffer
    }
}>

export const PgOutput = Data.taggedEnum<PgOutput>()

export enum MessageType {
    Relation = "R",
    Begin = "B",
    Commit = "C",
    Insert = "I",
    Update = "U",
    Delete = "D",
    Unknown = "X",
    Keepalive = "k",
    XLogData = "w",
}

export const parseLSN = (lsn: string): bigint => {
    const [hi, lo] = lsn.split("/")
    return (BigInt(`0x${hi}`) << 32n) | BigInt(`0x${lo}`)
}

export const formatLSN = (lsn: bigint): string =>
    `${(lsn >> 32n).toString(16).toUpperCase()}/${(lsn & 0xFFFFFFFFn).toString(16).toUpperCase()}`


export const decodeTupleData = (tupleData: Buffer): Result.Result<{ tupleData: TupleData, finalOffset: number }, PgReplError> => {
    let offset = 0
    const numberOfColumns = tupleData.readUInt16BE(offset)
    offset += 2

    const columns: Column[] = []
    for (let i = 0; i < numberOfColumns; i++) {
        const dataType = String.fromCharCode(tupleData[offset++])
        switch (dataType) {
            case "n":
                columns.push(Column.Null())
                break
            case "u":
                columns.push(Column.Toast())
                break
            case "t":
            case "b": {
                const length = tupleData.readUInt32BE(offset)
                offset += 4
                const value = tupleData.subarray(offset, offset + length)
                offset += length
                columns.push(dataType === "t"
                    ? Column.Text({ value: value.toString("utf-8") })
                    : Column.Binary({ value }))
                break
            }
            default:
                return Result.fail(new PgReplError({ message: `unknown data type: ${dataType}` }))
        }
    }

    return Result.succeed({
        tupleData: { numberOfColumns, columns },
        finalOffset: offset
    })
}



export const decodeRelatioData = (relationData: Buffer): RelationData => {
    let offset = 0
    const relationId = relationData.readUInt32BE(offset)
    offset += 4
    const [namespace, namespaceLen] = readCString(relationData, offset)
    offset = namespaceLen

    const [name, nameLen] = readCString(relationData, offset)
    offset = nameLen

    const replicaIdentity = relationData.readUInt8(offset++)
    const numberOfColumns = relationData.readUInt16BE(offset)
    offset += 2

    const columns = numberOfColumns === 0 ? [] : Array.makeBy<RelationColumn>(numberOfColumns, () => {
        const flag = relationData.readUInt8(offset++)
        const [name, columnNameLen] = readCString(relationData, offset)
        offset = columnNameLen

        const dataTypeOID = relationData.readUInt32BE(offset)
        offset += 4

        const dataTypeModifier = relationData.readInt32BE(offset)
        offset += 4

        return { flag, name, dataTypeOID, dataTypeModifier }
    })

    return { relationId, namespace, name, replicaIdentity, numberOfColumns, relationColumns: columns }

}

const decodeWalData = (walData: Buffer): Result.Result<PgOutput, PgReplError> => {
    const firstByte = String.fromCharCode(walData[0]) as MessageType
    switch (firstByte) {
        case MessageType.Relation:
            return Result.succeed(PgOutput.Relation(decodeRelatioData(walData.subarray(1))))

        case MessageType.Begin:
            return Result.succeed(PgOutput.Begin({
                finalLSN: walData.readBigUInt64BE(1),
                commitTimestamp: walData.readBigUInt64BE(9),
                xid: walData.readUInt32BE(17),
            }))
        case MessageType.Commit:
            return Result.succeed(PgOutput.Commit({
                flags: walData.readUInt8(1),
                commitLSN: walData.readBigUInt64BE(2),
                endLSN: walData.readBigUInt64BE(10),
                commitTimestamp: pgTimeToDate(walData.readBigUInt64BE(18)),
            }))

        case MessageType.Insert:
            if (String.fromCharCode(walData[5]) !== "N") {
                return Result.fail(new PgReplError({ message: "INSERT: expected 'N' tuple" }))
            }
            return Result.map(decodeTupleData(walData.subarray(6)), (tupleData) =>
                PgOutput.Insert({
                    relationId: walData.readUInt32BE(1),
                    tupleData: tupleData.tupleData,
                }))
        case MessageType.Update: {
            const relationId = walData.readUInt32BE(1)
            let offset = 5
            let marker = String.fromCharCode(walData[offset++])
            let oldTupleData: TupleData | undefined
            let oldTupleKind: "K" | "O" | undefined

            if (marker === "K" || marker === "O") {
                const result = decodeTupleData(walData.subarray(offset))

                if (Result.isFailure(result)) {
                    return Result.fail(new PgReplError({ message: `UPDATE: failed to decode old tuple`, cause: result }))
                }

                oldTupleData = result.success.tupleData
                oldTupleKind = marker
                offset += result.success.finalOffset
                marker = String.fromCharCode(walData[offset++])
            }

            if (marker !== "N") {
                return Result.fail(new PgReplError({ message: `UPDATE: expected 'N' tuple, got ${marker}` }))
            }

            return Result.map(decodeTupleData(walData.subarray(offset)), (result) => PgOutput.Update({
                relationId,
                oldTupleKind,
                oldTupleData,
                newTupleData: result.tupleData,
            }))
        }
        case MessageType.Delete: {
            const relationId = walData.readUInt32BE(1)
            const marker = String.fromCharCode(walData[5])

            if (marker === "K" || marker === "O") {
                const result = decodeTupleData(walData.subarray(6))

                if (Result.isFailure(result)) {
                    return Result.fail(new PgReplError({ message: `DELETE: failed to decode old tuple`, cause: result }))
                }
                return Result.succeed(PgOutput.Delete({
                    relationId,
                    oldTupleKind: marker,
                    oldTupleData: result.success.tupleData,
                }))
            }
        }
        default:
            return Result.succeed(PgOutput.Unknown({ type: firstByte, walData }))
    }
}

export const decodePgOutput = Match.type<CopyData>().pipe(
    Match.withReturnType<Result.Result<PgOutput, PgReplError>>(),
    Match.tag("Keepalive", (k) =>
        Result.succeed(PgOutput.Keepalive({
            serverWalEnd: k.serverWalEnd,
            serverTime: k.serverTime,
            replyRequested: k.replyRequested,
        }))),
    Match.tag("XLogData", (x) => decodeWalData(x.walData)),
    Match.exhaustive,
)

export const decodeCopyData = (chunk: Buffer): Result.Result<CopyData, PgReplError> => {
    const code = String.fromCharCode(chunk[0])
    switch (code) {
        case CopyDataCode.Keepalive:
            return Result.succeed(CopyData.Keepalive({
                serverWalEnd: chunk.readBigUInt64BE(1),
                serverTime: pgTimeToDate(chunk.readBigUInt64BE(9)),
                replyRequested: chunk[17] !== 0
            }))
        case CopyDataCode.XLogData:
            return Result.succeed(CopyData.XLogData({
                serverWalStart: chunk.readBigUInt64BE(1),
                serverWalEnd: chunk.readBigUInt64BE(9),
                serverTime: chunk.readBigUInt64BE(17),
                walData: chunk.subarray(25)
            }))
        default:
            return Result.fail(new PgReplError({ message: `unknown copy data code: ${code}`, cause: chunk }))
    }
}