/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) Daniel Stenberg, <daniel@haxx.se>, et al.
 *
 * This software is licensed as described in the file COPYING, which
 * you should have received as part of this distribution. The terms
 * are also available at https://curl.se/docs/copyright.html.
 *
 * You may opt to use, copy, modify, merge, publish, distribute and/or sell
 * copies of the Software, and permit persons to whom the Software is
 * furnished to do so, under the terms of the COPYING file.
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY
 * KIND, either express or implied.
 *
 * SPDX-License-Identifier: curl
 *
 ***************************************************************************/
#include "unitcheck.h"

#ifndef CURL_DISABLE_WEBSOCKETS
#include "urldata.h"
#include "multihandle.h"
#include "sendf.h"
#include "url.h"
#include "ws.h"

struct t2607_ctx {
  struct Curl_easy *easy;
  struct Curl_multi *multi;
  struct connectdata conn;
  size_t received;
  size_t sent;
  size_t reply_after;
  unsigned int blocks;
  size_t short_send;
  CURLcode send_error;
  bool coalesced;
  unsigned char output[32];
};

static CURLcode t2607_recv(struct Curl_easy *data, int8_t sockindex,
                            char *buffer, size_t length, size_t *received)
{
  static const char frames[] = "\x89\x04ping\x81\x05reply";
  struct t2607_ctx *ctx = data->set.private_data;
  size_t available = ctx->coalesced || ctx->sent >= ctx->reply_after ?
    sizeof(frames) - 1 : 6;
  (void)sockindex;
  *received = 0;
  if(ctx->received >= available)
    return CURLE_AGAIN;
  length = CURLMIN(length, available - ctx->received);
  memcpy(buffer, frames + ctx->received, length);
  ctx->received += length;
  *received = length;
  return CURLE_OK;
}

static CURLcode t2607_send(struct Curl_easy *data, int8_t sockindex,
                            const uint8_t *buffer, size_t length, bool eos,
                            size_t *written)
{
  struct t2607_ctx *ctx = data->set.private_data;
  (void)sockindex;
  (void)eos;
  *written = 0;
  if(ctx->send_error)
    return ctx->send_error;
  if(ctx->blocks) {
    ctx->blocks--;
    return CURLE_AGAIN;
  }
  if(ctx->short_send) {
    length = CURLMIN(length, ctx->short_send);
    ctx->short_send = 0;
    ctx->blocks = 1;
  }
  if(length > sizeof(ctx->output) - ctx->sent)
    return CURLE_SEND_ERROR;
  memcpy(ctx->output + ctx->sent, buffer, length);
  ctx->sent += length;
  *written = length;
  return CURLE_OK;
}

static void t2607_meta_free(void *entry)
{
  (void)entry;
  fail("connection metadata must supply its own destructor");
}

static void t2607_stop(struct t2607_ctx *ctx)
{
  if(ctx->easy) {
    ctx->easy->conn = NULL;
    Curl_client_cleanup(ctx->easy);
    if(ctx->multi) {
      if(ctx->conn.cpid != UINT32_MAX)
        Curl_uint32_tbl_remove(&ctx->multi->cpool.conns, ctx->conn.cpid);
      curl_multi_remove_handle(ctx->multi, ctx->easy);
    }
    curl_easy_cleanup(ctx->easy);
  }
  Curl_hash_destroy(&ctx->conn.meta_hash);
  curl_multi_cleanup(ctx->multi);
}

static CURLcode t2607_setup(struct t2607_ctx *ctx)
{
  CURLcode result;
  memset(ctx, 0, sizeof(*ctx));
  ctx->reply_after = 10;
  ctx->conn.cpid = UINT32_MAX;
  Curl_hash_init(&ctx->conn.meta_hash, 23, CURL_HASH_TYPE_BYTES,
                 t2607_meta_free);
  ctx->easy = curl_easy_init();
  ctx->multi = curl_multi_init();
  if(!ctx->easy || !ctx->multi)
    return CURLE_OUT_OF_MEMORY;
  if(curl_multi_add_handle(ctx->multi, ctx->easy))
    return CURLE_FAILED_INIT;
  result = curl_easy_setopt(ctx->easy, CURLOPT_CONNECT_ONLY, 2L);
  if(result)
    return result;
  result = curl_easy_setopt(ctx->easy, CURLOPT_PRIVATE, ctx);
  if(result)
    return result;

  /* Register a fake transport so the public receive API uses the real
   * decoder without socket scheduling, sleeps, or an external server. */
  result = Curl_uint32_tbl_resize(&ctx->multi->cpool.conns, 1);
  if(result)
    return result;
  if(!Curl_uint32_tbl_add(&ctx->multi->cpool.conns,
                         &ctx->conn, &ctx->conn.cpid))
    return CURLE_FAILED_INIT;
  ctx->conn.connection_id = 1;
  ctx->conn.sock[FIRSTSOCKET] = 42;
  ctx->conn.recv[FIRSTSOCKET] = t2607_recv;
  ctx->conn.send[FIRSTSOCKET] = t2607_send;
  ctx->easy->conn = &ctx->conn;
  ctx->easy->state.last_cpid = ctx->conn.cpid;
  ctx->easy->state.last_conn_id = ctx->conn.connection_id;
  return Curl_ws_accept(ctx->easy, NULL, 0);
}

static void t2607_run(unsigned int scenario)
{
  struct t2607_ctx ctx;
  const struct curl_ws_frame *meta = NULL;
  char buffer[16];
  size_t received = 0, written = 0;
  unsigned int attempts, i;
  CURLcode result = t2607_setup(&ctx);

  abort_unless(!result, "WebSocket setup failed");
  if(scenario == 6) {
    /* Receiving must not flush unreported application payload: doing so
     * would make a later curl_ws_send() retry encode the same frame again. */
    ctx.easy->set.ws_no_auto_pong = TRUE;
    ctx.blocks = 1;
    result = curl_ws_send(ctx.easy, "data", 4, &written, 0, CURLWS_BINARY);
    abort_unless(result == CURLE_AGAIN && !written,
                 "application send blocked");
    result = curl_ws_recv(ctx.easy, buffer, sizeof(buffer), &received, &meta);
    abort_unless(!result && meta,
                 "PING received while application send waits");
    fail_unless(meta->flags == CURLWS_PING, "manual PING received");
    fail_unless(!ctx.sent, "application send remains owned by its caller");
    result = curl_ws_send(ctx.easy, "data", 4, &written, 0, CURLWS_BINARY);
    fail_unless(!result && written == 4, "application send retry completed");
    fail_unless(ctx.sent == 10 && ctx.output[0] == 0x82,
                "application frame sent exactly once");
    goto unit_test_abort;
  }
  if(scenario == 1)
    ctx.blocks = 2;
  if(scenario == 2)
    ctx.short_send = 2;
  if(scenario == 3)
    ctx.send_error = CURLE_SEND_ERROR;
  if(scenario == 4)
    ctx.easy->set.ws_no_auto_pong = TRUE;
  if(scenario == 5)
    ctx.coalesced = TRUE;

  for(attempts = 0; attempts < 8; attempts++) {
    result = curl_ws_recv(ctx.easy, buffer, sizeof(buffer), &received, &meta);
    if(result != CURLE_AGAIN)
      break;
    fail_unless(!received, "EAGAIN must not return application data");
  }

  if(scenario == 3) {
    fail_unless(result == CURLE_SEND_ERROR, "PONG send error propagated");
    goto unit_test_abort;
  }
  abort_unless(!result, "receive must finish without another server frame");
  abort_unless(meta, "successful receive needs frame metadata");
  if(scenario == 4) {
    fail_unless(meta->flags == CURLWS_PING, "manual PING returned to caller");
    fail_unless(received == 4 && !memcmp(buffer, "ping", 4), "PING payload");
    fail_unless(!ctx.sent, "automatic PONG stays disabled");
    goto unit_test_abort;
  }

  fail_unless(meta->flags == CURLWS_TEXT, "application TEXT frame received");
  fail_unless(received == 5 && !memcmp(buffer, "reply", 5), "TEXT payload");
  abort_unless(ctx.sent == 10, "exactly one complete masked PONG sent");
  fail_unless(ctx.output[0] == 0x8a && ctx.output[1] == 0x84,
              "PONG opcode, mask bit and payload length");
  for(i = 0; i < 4; i++)
    fail_unless((ctx.output[6 + i] ^ ctx.output[2 + i]) == "ping"[i],
                "PONG echoes the PING payload");

unit_test_abort:
  t2607_stop(&ctx);
}

struct t2607_app_frame {
  unsigned int flags;
  unsigned char firstbyte;
  const char *payload;
  size_t length;
};

static void t2607_check_frame(struct t2607_ctx *ctx, size_t offset,
                               unsigned char firstbyte,
                               const char *payload, size_t length)
{
  size_t i;

  abort_unless(offset + 6 + length <= ctx->sent, "complete frame sent");
  fail_unless(ctx->output[offset] == firstbyte, "frame opcode preserved");
  fail_unless(ctx->output[offset + 1] == (0x80 | length),
              "frame mask bit and payload length preserved");
  for(i = 0; i < length; i++)
    fail_unless((ctx->output[offset + 6 + i] ^
                 ctx->output[offset + 2 + i % 4]) ==
                (unsigned char)payload[i], "frame payload preserved");
unit_test_abort:
  return;
}

static void t2607_application_send(const struct t2607_app_frame *frame,
                                    bool short_header, bool auto_pong,
                                    bool coalesced)
{
  struct t2607_ctx ctx;
  const struct curl_ws_frame *meta = NULL;
  char buffer[16];
  size_t written = 0, received = 0, frame_size = 6 + frame->length;
  size_t prefix = short_header ? 2 : 0;
  size_t expected = frame_size + (auto_pong ? 10 : 0);
  int failures = unitfail;
  CURLcode result = t2607_setup(&ctx);

  abort_unless(!result, "WebSocket setup failed");
  ctx.reply_after = SIZE_MAX;
  ctx.coalesced = coalesced;
  ctx.easy->set.ws_no_auto_pong = !auto_pong;
  if(short_header)
    ctx.short_send = prefix;
  else
    ctx.blocks = 1;
  result = curl_ws_send(ctx.easy, frame->payload, frame->length, &written,
                        0, frame->flags);
  abort_unless(result == CURLE_AGAIN && !written, "application send blocked");
  fail_unless(ctx.sent == prefix, "only the expected header prefix sent");

  result = curl_ws_recv(ctx.easy, buffer, sizeof(buffer), &received, &meta);
  if(!auto_pong) {
    abort_unless(!result && meta && meta->flags == CURLWS_PING,
                 "manual PING received during application send");
  }
  else if(coalesced) {
    abort_unless(!result && meta && meta->flags == CURLWS_TEXT,
                 "TEXT after automatic PING received");
  }
  else {
    fail_unless(result == CURLE_AGAIN && !received,
                "PING-only receive waits for application data");
  }
  fail_unless(ctx.sent == prefix,
              "receive leaves application-owned output for send retry");

  result = curl_ws_send(ctx.easy, frame->payload, frame->length, &written,
                        0, frame->flags);
  fail_unless(!result && written == frame->length,
              "application send retry completed");
  fail_unless(ctx.sent == expected, "one application frame sent after retry");
  t2607_check_frame(&ctx, 0, frame->firstbyte, frame->payload, frame->length);
  if(auto_pong)
    t2607_check_frame(&ctx, frame_size, 0x8a, "ping", 4);

  /* Completing the retry must also release ownership for the next message,
   * even when both messages have an empty payload and identical flags. */
  result = curl_ws_send(ctx.easy, frame->payload, frame->length, &written,
                        0, frame->flags);
  fail_unless(!result && written == frame->length, "next message completed");
  fail_unless(ctx.sent == expected + frame_size, "next frame sent once");
  t2607_check_frame(&ctx, expected, frame->firstbyte,
                    frame->payload, frame->length);

unit_test_abort:
  if(unitfail != failures)
    curl_mfprintf(stderr,
                  "flags=%u length=%zu short=%d auto=%d coalesced=%d\n",
                  frame->flags, frame->length, short_header, auto_pong,
                  coalesced);
  t2607_stop(&ctx);
}

static void t2607_control_retry(void)
{
  struct t2607_ctx ctx;
  const struct curl_ws_frame *meta = NULL;
  char buffer[16];
  size_t received = 0, written = 0;
  CURLcode result = t2607_setup(&ctx);

  abort_unless(!result, "WebSocket setup failed");
  ctx.blocks = 2;
  result = curl_ws_recv(ctx.easy, buffer, sizeof(buffer), &received, &meta);
  abort_unless(result == CURLE_AGAIN && !ctx.sent, "automatic PONG blocked");
  result = curl_ws_send(ctx.easy, "data", 4, &written, 0, CURLWS_TEXT);
  fail_unless(result == CURLE_AGAIN && !written,
              "application send waits for the queued automatic PONG");
  result = curl_ws_recv(ctx.easy, buffer, sizeof(buffer), &received, &meta);
  abort_unless(!result && meta && meta->flags == CURLWS_TEXT,
               "receive can retry control output before application encoding");
  fail_unless(ctx.sent == 10, "one automatic PONG sent");
  t2607_check_frame(&ctx, 0, 0x8a, "ping", 4);
  result = curl_ws_send(ctx.easy, "data", 4, &written, 0, CURLWS_TEXT);
  fail_unless(!result && written == 4, "application frame sent afterwards");
  fail_unless(ctx.sent == 20, "one PONG and one application frame sent");
  t2607_check_frame(&ctx, 10, 0x81, "data", 4);

unit_test_abort:
  t2607_stop(&ctx);
}

static void t2607_partial_frame(void)
{
  struct t2607_ctx ctx;
  const struct curl_ws_frame *meta = NULL;
  char buffer[16];
  size_t received = 0, written = 0;
  CURLcode result = t2607_setup(&ctx);

  abort_unless(!result, "WebSocket setup failed");
  ctx.reply_after = SIZE_MAX;
  result = curl_ws_send(ctx.easy, "da", 2, &written, 4,
                        CURLWS_TEXT | CURLWS_OFFSET);
  abort_unless(!result && written == 2, "first frame piece sent");
  fail_unless(ctx.sent == 8, "only the frame header and first piece sent");
  result = curl_ws_recv(ctx.easy, buffer, sizeof(buffer), &received, &meta);
  fail_unless(result == CURLE_AGAIN && !received, "PING consumed internally");
  fail_unless(ctx.sent == 8, "PONG must not interrupt an application frame");
  result = curl_ws_send(ctx.easy, "ta", 2, &written, 0, CURLWS_OFFSET);
  fail_unless(!result && written == 2, "remaining frame piece sent");
  fail_unless(ctx.sent == 20, "complete application frame followed by PONG");
  t2607_check_frame(&ctx, 0, 0x81, "data", 4);
  t2607_check_frame(&ctx, 10, 0x8a, "ping", 4);

unit_test_abort:
  t2607_stop(&ctx);
}
#endif

static CURLcode test_unit2607(const char *arg)
{
  UNITTEST_BEGIN_SIMPLE
#ifndef CURL_DISABLE_WEBSOCKETS
  static const struct t2607_app_frame frames[] = {
    { CURLWS_TEXT, 0x81, "", 0 },
    { CURLWS_BINARY, 0x82, "", 0 },
    { CURLWS_PING, 0x89, "", 0 },
    { CURLWS_PONG, 0x8a, "", 0 },
    { CURLWS_CLOSE, 0x88, "", 0 },
    { CURLWS_TEXT, 0x81, "data", 4 },
    { CURLWS_BINARY, 0x82, "data", 4 },
    { CURLWS_PING, 0x89, "data", 4 },
    { CURLWS_PONG, 0x8a, "data", 4 },
    { CURLWS_CLOSE, 0x88, "\x03\xe8ok", 4 }
  };
  unsigned int scenario, options;
  size_t i;

  abort_unless(!curl_global_init(CURL_GLOBAL_ALL), "global initialization");
  for(scenario = 0; scenario < 7; scenario++)
    t2607_run(scenario);
  for(i = 0; i < CURL_ARRAYSIZE(frames); i++) {
    for(options = 0; options < 8; options++)
      t2607_application_send(&frames[i], (options & 1) != 0,
                             (options & 2) != 0, (options & 4) != 0);
  }
  t2607_control_retry();
  t2607_partial_frame();
  curl_global_cleanup();
#endif
  UNITTEST_END_SIMPLE
}
