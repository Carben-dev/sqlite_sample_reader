//
//  db_utils.h
//  db_reader
//
//  Created by HUADONG XING on 2019-02-08.
//  Copyright © 2019 HUADONG XING. All rights reserved.
//
#include "database.h"

#ifndef db_utils_h
#define db_utils_h
char *read_page(int page_num);
struct sqlite_master *read_master_page(void);
struct master_header* read_master_header(void);
int decode_varint(uint8_t *, struct varint *);
int btol16(uint16_t big);
int btol32(uint32_t big);
int btol24(uint32_t big);

#endif /* db_utils_h */
