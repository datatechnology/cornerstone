/**
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  The ASF licenses
* this file to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#include "../../include/cornerstone.hxx"
#include <cassert>

using namespace cornerstone;

ulong long_val(int val) {
    ulong base = std::numeric_limits<uint>::max();
    return base + (ulong)val;
}

void test_serialization() {
    std::random_device engine;
    std::uniform_int_distribution<int32> distribution(1, 10000);
    auto rnd = std::bind(distribution, engine);

    ptr<srv_config> srv_conf(cs_new<srv_config>(rnd(), sstrfmt("server %d").fmt(rnd())));
    bufptr srv_conf_buf(srv_conf->serialize());
    ptr<srv_config> srv_conf1(srv_config::deserialize(*srv_conf_buf));
    assert(srv_conf->get_endpoint() == srv_conf1->get_endpoint());
    assert(srv_conf->get_id() == srv_conf1->get_id());

    ptr<cluster_config> conf(cs_new<cluster_config>(long_val(rnd()), long_val(rnd())));
    conf->get_servers().push_back(cs_new<srv_config>(rnd(), "server 1"));
    conf->get_servers().push_back(cs_new<srv_config>(rnd(), "server 2"));
    conf->get_servers().push_back(cs_new<srv_config>(rnd(), "server 3"));
    conf->get_servers().push_back(cs_new<srv_config>(rnd(), "server 4"));
    conf->get_servers().push_back(cs_new<srv_config>(rnd(), "server 5"));

    // test cluster config serialization
    bufptr conf_buf(conf->serialize());
    ptr<cluster_config> conf1(cluster_config::deserialize(*conf_buf));
    assert(conf->get_log_idx() == conf1->get_log_idx());
    assert(conf->get_prev_log_idx() == conf1->get_prev_log_idx());
    assert(conf->get_servers().size() == conf1->get_servers().size());
    for (cluster_config::srv_itor it = conf->get_servers().begin(), it1 = conf1->get_servers().begin();
        it != conf->get_servers().end() && it1 != conf1->get_servers().end(); ++it, ++it1) {
        assert((*it)->get_id() == (*it1)->get_id());
        assert((*it)->get_endpoint() == (*it1)->get_endpoint());
    }

    // test snapshot serialization
    ptr<snapshot> snp(cs_new<snapshot>(long_val(rnd()), long_val(rnd()), conf, long_val(rnd())));
    bufptr snp_buf(snp->serialize());
    ptr<snapshot> snp1(snapshot::deserialize(*snp_buf));
    assert(snp->get_last_log_idx() == snp1->get_last_log_idx());
    assert(snp->get_last_log_term() == snp1->get_last_log_term());
    assert(snp->get_last_config()->get_servers().size() == snp1->get_last_config()->get_servers().size());
    assert(snp->get_last_config()->get_log_idx() == snp1->get_last_config()->get_log_idx());
    assert(snp->get_last_config()->get_prev_log_idx() == snp1->get_last_config()->get_prev_log_idx());
    for (cluster_config::srv_itor it = snp->get_last_config()->get_servers().begin(), it1 = snp1->get_last_config()->get_servers().begin();
        it != snp->get_last_config()->get_servers().end() && it1 != snp1->get_last_config()->get_servers().end(); ++it, ++it1) {
        assert((*it)->get_id() == (*it1)->get_id());
        assert((*it)->get_endpoint() == (*it1)->get_endpoint());
    }

    // test snapshot sync request serialization
    bool done = rnd() % 2 == 0;
    bufptr rnd_buf(buffer::alloc(rnd()));
    for (size_t i = 0; i < rnd_buf->size(); ++i) {
        rnd_buf->put((byte)(rnd()));
    }

    rnd_buf->pos(0);

    bufptr copy_of_rnd = buffer::copy(*rnd_buf);
    ptr<snapshot_sync_req> sync_req(cs_new<snapshot_sync_req>(snp, long_val(rnd()), std::move(rnd_buf), done));
    bufptr sync_req_buf(sync_req->serialize());
    ptr<snapshot_sync_req> sync_req1(snapshot_sync_req::deserialize(*sync_req_buf));
    assert(sync_req->get_offset() == sync_req1->get_offset());
    assert(done == sync_req1->is_done());
    snapshot& snp2(sync_req1->get_snapshot());
    assert(snp->get_last_log_idx() == snp2.get_last_log_idx());
    assert(snp->get_last_log_term() == snp2.get_last_log_term());
    assert(snp->get_last_config()->get_servers().size() == snp2.get_last_config()->get_servers().size());
    assert(snp->get_last_config()->get_log_idx() == snp2.get_last_config()->get_log_idx());
    assert(snp->get_last_config()->get_prev_log_idx() == snp2.get_last_config()->get_prev_log_idx());
    buffer& buf1 = sync_req1->get_data();
    assert(buf1.pos() == 0);
    assert(copy_of_rnd->size() == buf1.size());
    for (size_t i = 0; i < buf1.size(); ++i) {
        byte* d = copy_of_rnd->data();
        byte* d1 = buf1.data();
        assert(*(d + i) == *(d1 + i));
    }

    // test with zero buffer
    done = rnd() % 2 == 1;
    sync_req = cs_new<snapshot_sync_req>(snp, long_val(rnd()), buffer::alloc(0), done);
    sync_req_buf = sync_req->serialize();
    sync_req1 = snapshot_sync_req::deserialize(*sync_req_buf);
    assert(sync_req->get_offset() == sync_req1->get_offset());
    assert(done == sync_req1->is_done());
    assert(sync_req1->get_data().size() == 0);
    snapshot& snp3(sync_req1->get_snapshot());
    assert(snp->get_last_log_idx() == snp3.get_last_log_idx());
    assert(snp->get_last_log_term() == snp3.get_last_log_term());
    assert(snp->get_last_config()->get_servers().size() == snp3.get_last_config()->get_servers().size());
    assert(snp->get_last_config()->get_log_idx() == snp3.get_last_config()->get_log_idx());
    assert(snp->get_last_config()->get_prev_log_idx() == snp3.get_last_config()->get_prev_log_idx());

    // test log entry serialization and deserialization
    bufptr data = buffer::alloc(24 + rnd() % 100);
    for (size_t i = 0; i < data->size(); ++i) {
        data->put(static_cast<byte>(rnd() % 255));
    }

    ptr<log_entry> entry(cs_new<log_entry>(long_val(rnd()), std::move(data), static_cast<log_val_type>(1 + rnd() % 5)));
    bufptr buf2 = entry->serialize();
    ptr<log_entry> entry1(log_entry::deserialize(*buf2));
    assert(entry->get_term() == entry1->get_term());
    assert(entry->get_val_type() == entry1->get_val_type());
    assert(entry->get_buf().size() == entry1->get_buf().size());
    for (size_t i = 0; i < entry->get_buf().size(); ++i) {
        byte b1 = entry->get_buf().get_byte();
        byte b2 = entry1->get_buf().get_byte();
        assert(b1 == b2);
    }
}

