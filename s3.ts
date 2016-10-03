/// <reference path="../d/node.d.ts"/>

import * as zlib from 'zlib';
import {Optional} from './types';

function getContentTypeByFile(fileName) {
    var rc = 'application/octet-stream';
    var fn = fileName.toLowerCase();

    if (fn.indexOf('.html') >= 0) rc = 'text/html';
    else if (fn.indexOf('.css') >= 0) rc = 'text/css';
    else if (fn.indexOf('.json') >= 0) rc = 'application/json';
    else if (fn.indexOf('.js') >= 0) rc = 'application/x-javascript';
    else if (fn.indexOf('.png') >= 0) rc = 'image/png';
    else if (fn.indexOf('.jpg') >= 0) rc = 'image/jpg';

    return rc;
}

function getCacheControl(fileName: string) {
    function match(str: string) {
        return fileName.toLowerCase().endsWith(str);
    }
    if (['.html', '.json', '.log'].some(match)) {
        return 'no-cache';
    } else {
        return 'max-age=31536000';
    }
}

type S3Error = {code: string, message: string};

export function exec(s3: any, method: string, param: {}): Promise<any> {
    return new Promise((resolve, reject) => {
        s3[method].apply(s3, [param, (err: S3Error, data) => {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        }]);
    });
}

function gzip(src: Buffer) {
    return new Promise<Buffer>((resolve, reject) => {
        zlib.gzip(src, (err, buf: Buffer) => {
            if (err) {
                reject(err);
            } else {
                resolve(buf);
            }
        });
    });
}

export function putObject(s3: any, bucket: string, src: Buffer|ArrayBuffer, toPath: string, metadata?: {}) {
        let param = {
            Bucket: bucket,
            Key: toPath,
            Body: src,
            Metadata: metadata,
            ContentType: getContentTypeByFile(toPath),
            CacheControl: getCacheControl(toPath),
        };
        return exec(s3, 'putObject', param).then(res => toPath);
}

export function putObjectWithZip(s3: any, bucket: string, src: Buffer, toPath: string, metadata?: {}) {
    return gzip(src).then(buf => {
        let param = {
            Bucket: bucket,
            Key: toPath,
            Body: src.length > buf.length ? buf : src,
            Metadata: metadata,
            ContentType: getContentTypeByFile(toPath),
            CacheControl: getCacheControl(toPath),
            ContentEncoding: src.length > buf.length ? 'gzip' : null,
        }
        return exec(s3, 'putObject', param).then(res => toPath);
    });
}

type HeadResult = {
    AcceptRanges: string,
    LastModified: string,
    ContentLength: string,
    ETag: string,
    CacheControl: string,
    ContentEncoding: string,
    ContentType: string,
    Metadata: {}
};
export function headObject(s3: any, bucket: string, path: string): Promise<Optional<HeadResult>> {
    return exec(s3, 'headObject', {Bucket: bucket, Key: path})
    .then(res => Optional.of(res))
    .catch(err => {
        if (err && err.code == 'NotFound') {
            return Optional.empty();
        } else {
            return new Promise((resolve, reject) => reject(err));
        }
    });
}

type S3Object = {
    AcceptRanges: string,
    LastModified: string,
    ContentLength: string,
    ETag: string,
    CacheControl: string,
    ContentType: string,
    Metadata: {},
    Body: Buffer
};
export function getObject(s3: any, bucket: string, path: string): Promise<Optional<S3Object>> {
    return exec(s3, 'getObject', {Bucket: bucket, Key: path})
    .then(res => Optional.of(res))
    .catch(err => {
        if (err && err.code == 'NoSuchKey') {
            return Optional.empty();
        } else {
            return new Promise((resolve, reject) => reject(err));
        }
    });
}

type ListResult = {
    Key: string,
    LastModified: Date,
    ETag: string,
    Size: number,
    StorageClass: string
};
export function listObjects(s3: any, bucket: string, prefix: string): Promise<ListResult[]> {
    return exec(s3, 'listObjectsV2', {Bucket: bucket, Prefix: prefix})
    .then(res => res.Contents);
}

type DeleteResult = {
    DeleteMarker: boolean,
    VersionId: string,
    RequestCharged: string
};
export function deleteObject(s3, bucket: string, path: string): Promise<DeleteResult> {
    return exec(s3, 'deleteObject', {Bucket: bucket, Key: path});
}
