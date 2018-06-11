#include <cstdint>
#include <numeric>
#include <algorithm>


typedef double data_type;
typedef double diff_type;
typedef std::uint32_t time_type;


extern "C" int allign_times(const time_type * ref_time,
                            const int ref_time_sz,
                            const data_type * data,
                            const int data_sz,
                            const time_type * times,
                            data_type * result)
{
    if (ref_time[ref_time_sz - 1] < times[data_sz - 1])
        return 1;

    const time_type * cref_time = ref_time;
    const data_type * cdata = data;
    data_type * cresult = result;

    for(const time_type * ctimes = times ; ctimes < times + data_sz;
        ++ctimes, ++cdata, ++cref_time, ++cresult)
    {
        for(;*cref_time < *ctimes; ++cref_time, ++cresult);
        if(*cref_time != *ctimes)
            return 1;
        *cresult = *cdata;
    }
    return 0;
}


extern "C" int make_diff(diff_type * diff,
                         const int diff_size,
                         const diff_type top_percentile,
                         const int min_good_window) {
    if (min_good_window <= 1)
        return 1;

    bool in_noisy_part = false;
    diff_type * noisy_start_at = nullptr;
    diff_type * clean_start_at = nullptr;

    for(data_type * cdiff = diff; cdiff != diff + diff_size ; ++cdiff) {
        if (*cdiff > 0 and *cdiff < top_percentile) {
            // good point
            if (in_noisy_part) {
                // inside bad region
                if (nullptr == clean_start_at) {
                    // this is the first clean point, set clean window start position
                    clean_start_at = cdiff;
                } else if (cdiff - clean_start_at == min_good_window) {
                    // get enought good points - close and recalculate bad region
                    auto new_diff = std::accumulate(noisy_start_at, clean_start_at, 0) / (clean_start_at - noisy_start_at);
                    // fill noisy region with new value
                    std::fill(noisy_start_at, clean_start_at, (new_diff < 0 ? 0 : new_diff));

                    in_noisy_part = false;
                    noisy_start_at = nullptr;
                    clean_start_at = nullptr;
                }
            }
            // if good point in good region - nothing to do
        } else {
            // bag point
            if (in_noisy_part) {
                // reset clean window flag just in case
                clean_start_at = nullptr;
            } else {
                // entet noisy region
                in_noisy_part = true;
                noisy_start_at = cdiff;
            }
        }
    }

    // no errors
    return 0;
}

