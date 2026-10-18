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

#define T2608_PAYLOAD (256 * 1024)

struct t2608_ctx {
  struct Curl_easy *easy;
  struct Curl_multi *multi;
  struct connectdata conn;
  size_t received;
  size_t first_read;
  unsigned int calls;
  bool block_once;
};

static CURLcode t2608_recv(struct Curl_easy *data, int8_t sockindex,
                            char *buffer, size_t length, size_t *received)
{
  static const unsigned char header[] = {
    0x82, 0x7f, 0, 0, 0, 0, 0, 4, 0, 0
  };
  struct t2608_ctx *ctx = data->set.private_data;
  size_t i, remaining = sizeof(header) + T2608_PAYLOAD - ctx->received;
  (void)sockindex;
  *received = 0;
  ctx->calls++;
  if(ctx->block_once && ctx->calls == 2)
    return CURLE_AGAIN;
  if(ctx->calls == 1 && ctx->first_read)
    length = CURLMIN(length, ctx->first_read);
  length = CURLMIN(length, remaining);
  for(i = 0; i < length; i++) {
    size_t pos = ctx->received + i;
    buffer[i] = pos < sizeof(header) ?
      (char)header[pos] : (char)((pos - sizeof(header)) % 251);
  }
  ctx->received += length;
  *received = length;
  return CURLE_OK;
}

static void t2608_meta_free(void *entry)
{
  (void)entry;
  fail("connection metadata must supply its own destructor");
}

static void t2608_stop(struct t2608_ctx *ctx)
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

static CURLcode t2608_setup(struct t2608_ctx *ctx)
{
  CURLcode result;
  memset(ctx, 0, sizeof(*ctx));
  ctx->conn.cpid = UINT32_MAX;
  Curl_hash_init(&ctx->conn.meta_hash, 23, CURL_HASH_TYPE_BYTES,
                 t2608_meta_free);
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
  ctx->conn.recv[FIRSTSOCKET] = t2608_recv;

  ctx->easy->conn = &ctx->conn;
  ctx->easy->state.last_cpid = ctx->conn.cpid;
  ctx->easy->state.last_conn_id = ctx->conn.connection_id;
  return Curl_ws_accept(ctx->easy, NULL, 0);
}

static void t2608_run(size_t first_read, bool block_once)
{
  struct t2608_ctx ctx;
  const struct curl_ws_frame *meta = NULL;
  char buffer[4096];
  size_t received = 0, offset = 0, i;
  unsigned int attempts;
  CURLcode result = t2608_setup(&ctx);

  abort_unless(!result, "WebSocket setup failed");
  ctx.first_read = first_read;
  ctx.block_once = block_once;
  for(attempts = 0; attempts < 3; attempts++) {
    result = curl_ws_recv(ctx.easy, buffer, 1, &received, &meta);
    if(result != CURLE_AGAIN)
      break;
    fail_unless(!received, "split header must not return application data");
  }
  abort_unless(!result && meta, "one-byte receive succeeded");
  fail_unless(received == 1 && buffer[0] == 0, "first payload byte");
  fail_unless(meta->offset == 0 &&
              meta->bytesleft == T2608_PAYLOAD - 1, "partial frame metadata");
  fail_unless(ctx.received < T2608_PAYLOAD / 2,
              "small receive must leave most input with the transport");
  offset = received;

  /* Resume until the entire large frame is consumed, checking that bounded
   * reads also preserve payload and offsets across all buffer boundaries. */
  while(offset < T2608_PAYLOAD) {
    result = curl_ws_recv(ctx.easy, buffer, sizeof(buffer), &received, &meta);
    if(result == CURLE_AGAIN)
      continue;
    abort_unless(!result && meta && received, "remaining payload received");
    fail_unless(meta->flags == CURLWS_BINARY, "binary frame flag preserved");
    fail_unless(meta->offset == (curl_off_t)offset, "frame offset preserved");
    abort_unless(received <= T2608_PAYLOAD - offset, "payload length bounded");
    fail_unless(meta->bytesleft ==
                (curl_off_t)(T2608_PAYLOAD - offset - received),
                "remaining payload length preserved");
    for(i = 0; i < received; i++)
      fail_unless((unsigned char)buffer[i] == (offset + i) % 251,
                  "payload preserved across receive chunks");
    offset += received;
  }
  fail_unless(ctx.received == T2608_PAYLOAD + 10, "complete frame consumed");

unit_test_abort:
  t2608_stop(&ctx);
}
#endif

static CURLcode test_unit2608(const char *arg)
{
  UNITTEST_BEGIN_SIMPLE
#ifndef CURL_DISABLE_WEBSOCKETS
  abort_unless(!curl_global_init(CURL_GLOBAL_ALL), "global initialization");
  t2608_run(0, FALSE);
  t2608_run(1, FALSE);
  t2608_run(3, TRUE);
  t2608_run(10, FALSE);
  curl_global_cleanup();
#endif
  UNITTEST_END_SIMPLE
}
