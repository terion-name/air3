export type HttpMethod = 'GET' | 'HEAD' | string;
export interface SignUrlInput {
    method: HttpMethod;
    baseUrl: string;
    server?: string;
    bucket: string;
    key: string;
    secret: string;
    expires: Date | number;
    range?: string;
    responseContentType?: string;
    responseContentDisposition?: string;
    /** Emit /{key} (or /{server}/{key} when server is set) while signing the canonical claims with bucket. */
    defaultBucketPath?: boolean;
}
export interface VerifyUrlInput {
    method: HttpMethod;
    url: string;
    server?: string;
    secret: string;
    now: Date | number;
    range?: string;
    /** Accept /{key} (or /{server}/{key} when server is set) paths and verify them against this real bucket. */
    defaultBucket?: string;
}
export interface Claims {
    method: 'GET' | 'HEAD';
    server?: string;
    bucket: string;
    key: string;
    expires: Date;
    range: string;
    responseContentType: string;
    responseContentDisposition: string;
}
export declare class EdgeSignError extends Error {
    constructor(message: string);
}
export declare class InvalidSignatureError extends EdgeSignError {
    constructor();
}
export declare class ExpiredSignatureError extends EdgeSignError {
    constructor();
}
export declare class UnsignedRangeError extends EdgeSignError {
    constructor();
}
export declare class RangeMismatchError extends EdgeSignError {
    constructor();
}
export declare function signUrl(input: SignUrlInput): string;
export declare function verifyUrl(input: VerifyUrlInput): Claims;
export declare function canonicalString(claims: Claims): string;
