#pragma once

#include "util_types.hpp"

namespace autocomplete {

template <typename ListType, typename RMQ>
struct unsorted_list {
    unsorted_list() {}

    void build(std::vector<id_type> const& list) {
        essentials::logger("building unsorted_list...");
        m_rmq.build(list, std::less<id_type>());
        m_list.build(list);
        essentials::logger("DONE");
    }

    uint32_t topk(range r, uint32_t k, std::vector<id_type>& topk,
                  bool unique = false  // return unique results
    ) {
        uint32_t range_len = r.end - r.begin;
        if (range_len <= k) {  // report everything in range
            for (uint32_t i = 0; i != range_len; ++i) {
                topk[i] = m_list.access(r.begin + i);
            }
            std::sort(topk.begin(), topk.begin() + range_len);
            return range_len;
        }

        scored_range sr;
        sr.r = {r.begin, r.end - 1};  // rmq needs inclusive ranges
        sr.min_pos = m_rmq.rmq(sr.r.begin, sr.r.end);
        sr.min_val = m_list.access(sr.min_pos);

        m_q.clear();
        m_q.push(sr);

        uint32_t i = 0;
        while (true) {
            scored_range min = m_q.top();

            if (!unique or
                (unique and !std::binary_search(topk.begin(), topk.begin() + i,
                                                min.min_val))) {
                topk[i++] = min.min_val;
            }

            if (i == k) break;
            m_q.pop();

            if (min.min_pos > 0 and min.min_pos - 1 >= min.r.begin) {
                scored_range left;
                left.r = {min.r.begin, min.min_pos - 1};
                // TODO: optimize for small ranges
                // compute rmq by scanning the range
                left.min_pos = m_rmq.rmq(left.r.begin, left.r.end);
                left.min_val = m_list.access(left.min_pos);
                m_q.push(left);
            }

            if (min.min_pos < size() - 1 and min.r.end >= min.min_pos + 1) {
                scored_range right;
                right.r = {min.min_pos + 1, min.r.end};
                // TODO: optimize for small ranges
                // compute rmq by scanning the range
                right.min_pos = m_rmq.rmq(right.r.begin, right.r.end);
                right.min_val = m_list.access(right.min_pos);
                m_q.push(right);
            }
        }

        return k;
    }

    size_t size() const {
        return m_list.size();
    }

    size_t bytes() const {
        return m_rmq.bytes() + m_list.bytes();
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_rmq);
        visitor.visit(m_list);
    }

private:
    topk_queue m_q;
    RMQ m_rmq;
    ListType m_list;
};

}  // namespace autocomplete