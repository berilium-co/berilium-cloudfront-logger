const { pipeline } = require('node:stream/promises')
const fs = require('node:fs')
const zlib = require('node:zlib')
const {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
} = require('@aws-sdk/client-s3')

const bucket = 'berilium-access-logs'

async function run() {
  const s3_client = new S3Client({ region: 'us-west-2' })
  const input = {
    // ListObjectsV2Request
    Bucket: bucket, // required
  }
  const command = new ListObjectsV2Command(input)
  const response = await s3_client.send(command)
  for (const object of response.Contents) {
    const filename = object.Key
    if (!filename.endsWith('.gz')) continue
    const command = new GetObjectCommand({
      // GetObjectRequest
      Bucket: bucket, // required
      Key: filename, // required
    })
    const response = await s3_client.send(command)
    await pipeline(
      response.Body,
      zlib.createUnzip(),
      async function* (source, { signal }) {
        source.setEncoding('utf8') // Work with strings rather than `Buffer`s.
        let buf = ''
        for await (const chunk of source) {
          buf = buf + chunk
          const lines = buf.split('\n')
          buf = lines.pop()
          for (const line of lines) {
            if (line.startsWith('#')) continue
            const words = line.split('\t')
            const [year, month, day] = words[0].split('-')
            const [hour, minute, second] = words[1].split(':')
            const timestamp = Date.UTC(
              year,
              month - 1,
              day,
              hour,
              minute,
              second,
            )
            console.log([
              timestamp,
              {
                filename: filename,
                x_edge_location: words[2],
                sc_bytes: Number.parseInt(words[3]),
                c_ip: words[4],
                cs_method: words[5],
                cs_host: words[6],
                cs_uri_stem: words[7],
                sc_status: Number.parseInt(words[8]),
                cs_referer: words[9],
                cs_user_agent: words[10],
                cs_uri_query: words[11],
                cs_Cookie: words[12],
                x_edge_result_type: words[13],
                x_edge_request_id: words[14],
                x_host_header: words[15],
                cs_protocol: words[16],
                cs_bytes: Number.parseInt(words[17]),
                time_taken: Number.parseFloat(words[18]),
                x_forwarded_for: words[19],
                ssl_protocol: words[20],
                ssl_cipher: words[21],
                x_edge_response_result_type: words[22],
                cs_protocol_version: words[23],
                fle_status: words[24],
                fle_encrypted_fields: words[25],
                c_port: Number.parseInt(words[26]),
                time_to_first_byte: Number.parseFloat(words[27]),
                x_edge_detailed_result_type: words[28],
                sc_content_type: words[29],
                sc_content_len: Number.parseInt(words[30]),
                sc_range_start: words[31],
                sc_range_end: words[32],
              },
            ])
          }
        }
      },
    )
  }
}

run().catch(console.error)
