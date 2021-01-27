#ifndef __PG_WEIGHTS_HEADER__
#define __PG_WEIGHTS_HEADER__

#ifdef __cplusplus
extern "C" {
#endif

#define BUFSIZE 64

static inline text *weights_to_json(bytea *bw) {
    lwdebug(1,"Enter weights_to_json().");
    uint8_t *w = (uint8_t*)VARDATA(bw);
    const uint8_t *pos = w;

    uint32_t idx = 0;
    uint16_t n_nbrs = 0;
    char w_type;

    memcpy(&w_type, pos, sizeof(char));
    pos += sizeof(char); // weights type char

    uint32_t num_obs = 0;
    memcpy(&num_obs, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t); // num_obs

    lwdebug(1, "wtype=%s num_obs=%d", w_type, num_obs);

    List *c_arr;
    size_t n_arr = 0, i, j, c_len;
    uint32_t nid;
    float nweights;
    char *c_nbrs;

    for (i = 0; i < num_obs; ++i) {
        c_len = 0;

        // read idx
        memcpy(&idx, pos, sizeof(uint32_t));
        pos += sizeof(uint32_t);

        // read number of neighbors
        memcpy(&n_nbrs, pos, sizeof(uint16_t));
        pos += sizeof(uint16_t);

        // create string memory
        c_nbrs = lwalloc(BUFSIZE * n_nbrs * sizeof(char));

        // assign string content
        snprintf(c_nbrs, BUFSIZE, "%d:", idx);

        if (w_type == 'w') {
            strcat(c_nbrs, "[");
        }
        strcat(c_nbrs, "[");

        // loop neighbrs
        for (j = 0; j < n_nbrs; ++j) {
            char buffer[BUFSIZE];
            // read neighbor id
            memcpy(&nid, pos, sizeof(uint32_t));
            pos += sizeof(uint32_t);

            snprintf(buffer, BUFSIZE, "%d", nid);

            strcat(c_nbrs, buffer);
            if (j < n_nbrs - 1)
                strcat(c_nbrs, ",");
        }

        strcat(c_nbrs, "]");

        if (w_type == 'w') {
            strcat(c_nbrs, ",[");
            for (j = 0; j < n_nbrs; ++j) {
                char buffer[BUFSIZE];
                // read neighbor weight
                memcpy(&nweights, pos, sizeof(float));
                pos += sizeof(float);

                snprintf(buffer, BUFSIZE, "%f", nweights);
                strcat(c_nbrs, buffer);
                if (j < n_nbrs - 1)
                    strcat(c_nbrs, ",");
            }
            strcat(c_nbrs, "]]");
        }

        if (i < num_obs - 1)
            strcat(c_nbrs, ",");

        n_arr += strlen(c_nbrs);

        if (i == 0) {
            c_arr = list_make1(c_nbrs);
        } else {
            c_arr = lappend(c_arr, c_nbrs);
        }

        lwdebug(1, "text=%s length=%d", c_nbrs, n_arr);
    }

    // compose returned json string
    char *w_str = lwalloc((n_arr + 2) * sizeof(char));
    w_str[0] = '{';

    ListCell *l;
    foreach (l, c_arr) {
        char *c_nbrs = (char *) (lfirst(l));
        strcat(w_str, c_nbrs);
        lwfree(c_nbrs);
    }
    strcat(w_str, "}");

    text *result;
    size_t len = strlen(w_str);
    lwdebug(1, "total length=%d", len);

    size_t res_len = len + VARHDRSZ;

    result = (text *) palloc(res_len);
    SET_VARSIZE(result, res_len);
    memcpy(VARDATA(result), w_str, len);

    lwfree(w_str);

    return result;
}

/**
 * The binary format of spatial weights
 *
 * char (1 bytes): weights type: 'a'->GAL 'w'->GWT
 * uint32 (4 bytes): number of observations: N
 * ...
 * uint32 (4 bytes): index of i-th observation
 * uint16 (2 bytes): number of neighbors of i-th observation (nn)
 * uint32 (4 bytes x nn): neighbor id
 * float (4 bytes x nn): weights value of each neighbor
 * ...
 *
 * total size of GAL weights = 1 + 4 + (2 + nn *(4+4)) * N
 * total size of GWT weights = 1 + 4 + (2 + nn * 4) * N
 *
 * e.g. 10 million observations, on average, each observation has nn neighbors:
 * if nn=20, gal weights, total size = 0.76GB
 * if nn=20, gwt weights, total size = 1.5GB
 *
 * e.g. 100 million observations, on average, each observation has nn neighbors:
 * if nn=20, gal weights, total size = 7.6GB
 * if nn=20, gwt weights, total size = 15.08GB
 *
 */
//uint8_t* weights_to_bytes(PGWeight *w, size_t *size_out);
static inline uint8_t *weights_to_bytes(PGWeight *w, size_t *size_out) {
    lwdebug(1,"Enter weights_to_bytes.");

    size_t buf_size = 0;
    uint8_t *buf;
    buf = NULL;

    /* Initialize output size */
    if (size_out) *size_out = 0;

    buf_size += sizeof(char); // weights type

    int32_t num_obs = w->num_obs;
    buf_size += sizeof(uint32_t); // num_obs

    for (size_t i = 0; i < num_obs; ++i) {
        buf_size += sizeof(uint32_t); // idx

        uint16_t num_nbrs = w->neighbors[i].num_nbrs;
        buf_size += sizeof(uint16_t); // num_nbrs

        buf_size = buf_size + sizeof(uint32_t) * num_nbrs;
        if (w->w_type == 'w') {
            buf_size = buf_size + sizeof(float) * num_nbrs;
        }
    }

    buf = lwalloc(buf_size);
    if (buf == NULL) {
        lwdebug(4, "Unable to allocate %d bytes for Weights output buffer.", buf_size);
        lwerror("Unable to allocate %d bytes for Weights output buffer.", buf_size);
        return NULL;
    }

    /* Retain a pointer to the front of the buffer for later */
    uint8_t *w_out = buf;

    memcpy(buf, (uint8_t *) (&w->w_type), sizeof(char)); // copy weights type
    buf += sizeof(char);

    memcpy(buf, (uint8_t *) (&num_obs), sizeof(uint32_t));  // copy num_obs
    buf += sizeof(uint32_t);

    for (size_t i = 0; i < num_obs; ++i) {
        memcpy(buf, (uint8_t *) (&w->neighbors[i].idx), sizeof(uint32_t)); // copy idx
        buf += sizeof(uint32_t);

        uint16_t num_nbrs = w->neighbors[i].num_nbrs;

        memcpy(buf, (uint8_t *) (&num_nbrs), sizeof(uint16_t)); // copy n_nbrs
        buf += sizeof(uint16_t);

        for (size_t j = 0; j < num_nbrs; ++j) { // copy nbr_id
            memcpy(buf, (uint8_t *) (&w->neighbors[i].nbrId[j]), sizeof(uint32_t));
            buf += sizeof(uint32_t);
        }

        if (w->w_type == 'w') {
            for (size_t j = 0; j < num_nbrs; ++j) { // copy nbr_weight
                memcpy(buf, (uint8_t *) (&w->neighbors[i].nbrWeight[j]), sizeof(float));
                buf += sizeof(float);
            }
        }
    }

    /* The buffer pointer should now land at the end of the allocated buffer space. Let's check. */
    if (buf_size != (size_t) (buf - w_out)) {
        lwdebug(4, "Output Weight is not the same size as the allocated buffer. %d=%d.", buf_size, buf - w_out);
        lwerror("Output Weight is not the same size as the allocated buffer. %d=%d", buf_size, buf - w_out);
        lwfree(w_out);
        return NULL;
    }

    /* Report output size */
    if (size_out) *size_out = buf_size;

    lwdebug(1,"Exit weights_to_bytes.");
    return w_out;
}

#ifdef __cplusplus
}
#endif

#endif