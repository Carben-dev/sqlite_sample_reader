//
//  db_utils.c
//  db_reader
//
//  Created by HUADONG XING on 2019-02-08.
//  Copyright © 2019 HUADONG XING. All rights reserved.
//

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "db_utils.h"
#include "database.h"

extern int db_mode;
extern int db_pg_size;
extern int db_fd;
extern unsigned int page_read_count;

// Read in page number, return the corresponing struct that match it's
// page type.
char *read_page(int page_num){
    char *page = malloc(db_pg_size);
    if (pread(db_fd, page, db_pg_size, page_num * db_pg_size) != db_pg_size) {
        perror("read_page");
        exit(-1);
    }
    
    page_read_count++;
    
    struct b_tree_header *header = (struct b_tree_header *)page;
    
    // handle clusted db
    if (db_mode == 2) {
        if (header->type_flag == 0x2) {
            char *cell_pointer = page + 12;
            
            struct clust_interior_cell *head = NULL;
            struct clust_interior_cell *prev = NULL;
            
            for (int i = 0; i < btol16(header->cell_count); i++) {
                
                int cell_offset = btol16(*(uint16_t *)cell_pointer);
                char *cell = page + cell_offset;
                
                struct clust_interior_cell *curr = malloc(sizeof(struct clust_interior_cell));
                
                if (i == 0) {
                    head = curr;
                } else {
                    prev->next = curr;
                }
                
                prev = curr;
                
                curr->type = header->type_flag;
                
                curr->page_num = btol32(*(int *)cell) - 1;
                cell += 4;
                
                struct varint payload_size;
                decode_varint((uint8_t *)cell, &payload_size);
                
                cell += payload_size.length_inbyte;
                
                
                struct varint hdr_size;
                decode_varint((uint8_t *)cell, &hdr_size);
                
                cell += hdr_size.value;
                
                curr->record.Emp_ID =  btol24(*((uint32_t *)cell));
                curr->key_emp_id = btol24(*((uint32_t *)cell));
                
                cell += 3;
                
                memcpy(curr->record.Name_Prefix, cell, 5);
                curr->record.Name_Prefix[5] = '\0';
                
                cell += 5;
                
                memcpy(curr->record.First_Name, cell, 11);
                curr->record.First_Name[11] = '\0';
                
                cell += 11;
                
                memcpy(curr->record.Middle_Initial, cell, 1);
                curr->record.Middle_Initial[1] = '\0';
                
                cell += 1;
                
                memcpy(curr->record.Last_Name, cell, 13);
                curr->record.Last_Name[13] = '\0';
                
                
                // update cell pointer
                cell_pointer += 2;
                
            }
            
            char *right_most_cell_pointer = page + 8;
            struct clust_interior_cell *right_most_cell = malloc(sizeof(struct clust_interior_cell));
            right_most_cell->type = header->type_flag;
            right_most_cell->page_num = btol32(*(int *)right_most_cell_pointer) - 1;
            right_most_cell->next = NULL;
            right_most_cell->key_emp_id = 0;
            prev->next = right_most_cell;
            
            
            return (char *)head;
        }
        
        if (header->type_flag == 0xa) {
            char *cell_pointer = page + 8;
            
            struct clust_leaf_cell *head = NULL;
            struct clust_leaf_cell *prev = NULL;
            
            for (int i = 0; i<btol16(header->cell_count); i++) {
                int cell_offset = btol16(*(uint16_t *)cell_pointer);
                char *cell = page + cell_offset;
                
                struct clust_leaf_cell *curr = malloc(sizeof(struct clust_leaf_cell));
                
                if (i == 0) {
                    head = curr;
                    
                } else {
                    prev->next = curr;
                }
                prev = curr;

                curr->type = header->type_flag;
                
                struct varint payload_size;
                decode_varint((uint8_t *)cell, &payload_size);
                
                cell += payload_size.length_inbyte;
                
                struct varint hdr_size;
                decode_varint((uint8_t *)cell, &hdr_size);
                
                cell += hdr_size.value;
                
                curr->record.Emp_ID = btol24(*((uint32_t *)cell));
                curr->key_emp_id = btol24(*((uint32_t *)cell));
                
                cell += 3;
                
                memcpy(curr->record.Name_Prefix, cell, 5);
                curr->record.Name_Prefix[5] = '\0';
                
                cell += 5;
                
                memcpy(curr->record.First_Name, cell, 11);
                curr->record.First_Name[11] = '\0';
                
                cell += 11;
                
                memcpy(curr->record.Middle_Initial, cell, 1);
                curr->record.Middle_Initial[1] = '\0';
                
                cell += 1;
                
                memcpy(curr->record.Last_Name, cell, 13);
                curr->record.Last_Name[13] = '\0';
                
                
                // update cell pointer
                cell_pointer += 2;
            }
            
            prev->next = NULL;
            
            return (char *)head;
        }
        
        printf("clust mode, page type not match.\n");
        exit(-1);
    }
    
    // type 0xd
    if (header->type_flag == 0x0d) {
        
        // for 0xd page, cell pointer start at offset 8
        char *cell_pointer = page + 8;
        
        // for all cell pointer, read each cell into struct
        struct table_leaf_cell *head = NULL;
        struct table_leaf_cell *prev = NULL;
        
        for (int i = 0; i < btol16(header->cell_count); i++) {
            
            int cell_offset = btol16(*(uint16_t *)cell_pointer);
            char *cell = page + cell_offset;
            
            struct table_leaf_cell *curr = malloc(sizeof(struct table_leaf_cell));
            if (i == 0) {
                head = curr;
                
            } else {
                prev->next = curr;
            }
            prev = curr;
            
            
            
            curr->type = header->type_flag;
            
            struct varint payload_size;
            decode_varint((uint8_t *)cell, &payload_size);
            curr->payload_size = payload_size.value;
            
            cell += payload_size.length_inbyte;
            
            struct varint rowid;
            decode_varint((uint8_t *)cell, &rowid);
            curr->rowid = rowid.value;
            
            cell += rowid.length_inbyte;
            
            struct varint hdr_size;
            decode_varint((uint8_t *)cell, &hdr_size);
            
            cell += hdr_size.value;
            
            curr->record.Emp_ID =  btol24(*((uint32_t *)cell));
            
            cell += 3;
            
            memcpy(curr->record.Name_Prefix, cell, 5);
            curr->record.Name_Prefix[5] = '\0';
            
            cell += 5;
            
            memcpy(curr->record.First_Name, cell, 11);
            curr->record.First_Name[11] = '\0';
            
            cell += 11;
            
            memcpy(curr->record.Middle_Initial, cell, 1);
            curr->record.Middle_Initial[1] = '\0';
            
            cell += 1;
            
            memcpy(curr->record.Last_Name, cell, 13);
            curr->record.Last_Name[13] = '\0';
            
            
            // update cell pointer
            cell_pointer += 2;
        }
        
        prev->next = NULL;
        
        return (char *)head;
    }
    
    // This is a 0x5(table interior) page
    if (header->type_flag == 0x5) {
        // for 0x5 page, cell pointer start at offset 12
        char *cell_pointer = page + 12;
        
        struct table_interior_cell *head = NULL;
        struct table_interior_cell *prev = NULL;
        
        for (int i = 0; i < btol16(header->cell_count); i++) {
            
            int cell_offset = btol16(*(uint16_t *)cell_pointer);
            char *cell = page + cell_offset;
            
            struct table_interior_cell *curr = malloc(sizeof(struct table_interior_cell));
            if (i == 0) {
                head = curr;
                
            } else {
                prev->next = curr;
            }
            prev = curr;
            
            
            curr->type = header->type_flag;
            
            curr->page_num = btol32(*((int *)cell)) - 1;
            cell += 4;
            
            struct varint rowid;
            decode_varint((uint8_t *)cell, &rowid);
            curr->key_rowid = rowid.value;
            
            // update cell pointer
            cell_pointer += 2;
        }
        
        // for 0x5, we still need to add the right most page
        int *right_most_cell_pointer = page + 8;
        struct table_interior_cell *right_most_cell = malloc(sizeof(struct table_interior_cell));
        right_most_cell->type = header->type_flag;
        right_most_cell->page_num = btol32(*right_most_cell_pointer) - 1;
        right_most_cell->key_rowid = 0;
        right_most_cell->next = NULL;
        prev->next = right_most_cell;
        
        return (char *)head;
        
    }
    
    // 0x2, This is a interior index page
    if (header->type_flag == 0x2) {
        // for interior page, cell pointer start at offset 12
        char *cell_pointer = page + 12;
        
        struct index_interior_cell *head = NULL;
        struct index_interior_cell *prev = NULL;
        
        for (int i = 0; i < btol16(header->cell_count); i++) {
            
            int cell_offset = btol16(*(uint16_t *)cell_pointer);
            char *cell = page + cell_offset;
            
            struct index_interior_cell *curr = malloc(sizeof(struct index_interior_cell));
            
            if (i == 0) {
                head = curr;
            } else {
                prev->next = curr;
            }
            
            prev = curr;
            
            curr->type = header->type_flag;
            
            curr->page_num = btol32(*(int *)cell) - 1;
            cell += 4;
            
            struct varint payload_size;
            decode_varint(cell, &payload_size);
            
            cell += payload_size.length_inbyte;
            
            struct varint hdr_size;
            decode_varint(cell, &hdr_size);
            
            cell += hdr_size.value;
            
            curr->key_emp_id = btol24(*(int *)cell);
            
            cell += 3;
            
            curr->rowid = btol24(*(int *)cell);
            
            // update cell pointer
            cell_pointer += 2;
            
        }
        
        // for interior node, we still need to add the right
        // most page.
        int *right_most_cell_pointer = page + 8;
        struct index_interior_cell *right_most_cell = malloc(sizeof(struct index_interior_cell));
        right_most_cell->type = header->type_flag;
        right_most_cell->page_num = btol32(*(int *)right_most_cell_pointer) - 1;
        right_most_cell->key_emp_id = 0;
        right_most_cell->rowid = 0;
        right_most_cell->next = NULL;
        
        prev->next = right_most_cell;
        
        return (char *)head;
        
        
    }
    
    // This is a leaf index page
    if (header->type_flag == 0xa) {
        
        // for leaf index page
        char *cell_pointer = page + 8;
        
        // for all cell pointer, read each cell into struct
        struct index_lead_cell *head = NULL;
        struct index_lead_cell *prev = NULL;
        
        for (int i = 0; i < btol16(header->cell_count); i++) {
            
            int cell_offset = btol16(*(uint16_t *)cell_pointer);
            char *cell = page + cell_offset;
            
            struct index_lead_cell *curr = malloc(sizeof(struct index_lead_cell));
            
            if (i == 0) {
                head = curr;
            } else {
                prev->next = curr;
            }
            
            prev = curr;
            
            curr->type = header->type_flag;
            
            struct varint payload_size;
            decode_varint((uint8_t *)cell, &payload_size);
            
            cell += payload_size.length_inbyte;
            
            struct varint hdr_size;
            decode_varint((char *)cell, &hdr_size);
            
            cell += hdr_size.length_inbyte;
            
            struct varint hdr_1;
            decode_varint(cell, &hdr_1);
            
            cell += hdr_1.length_inbyte;

            struct varint hdr_2;
            decode_varint(cell, &hdr_2);
            
            cell += hdr_2.length_inbyte;
            
            curr->key_emp_id = btol24(*(int *)cell);
            
            cell += 3;
            
            if (hdr_2.value == 3) {
                curr->rowid = btol24(*(int *)cell);
            }
            
            if (hdr_2.value == 2) {
                curr->rowid = btol16(*(int *)cell);
            }
            
            
            // update cell pointer
            cell_pointer += 2;
            
        }
        
        return (char *)head;
    }
    
    free(page);
    return NULL;
}

struct sqlite_master *read_master_page(void) {
    // read first page into memory
    char *page = malloc(db_pg_size);
    if (read(db_fd, page, db_pg_size) != db_pg_size) {
        perror("read_master_table");
        exit(-1);
    }
    
    struct b_tree_header *header = (struct b_tree_header *)(page + 100);
    
    // Since there is only one table (Employee) or one table + index in our db.
    // So, we only consider the case that the first page if 0d type.
    // Thus, the first cell pointer is at offset 8.
    
    char *cell_pointer = page + 100 + 8;
    
    struct table_interior_cell *head;
    struct table_interior_cell *prev;
    
    for (int i = 0; i < btol16(header->cell_count); i++) {
        
        int cell_offset = btol16(*(uint16_t *)cell_pointer);
        char *cell = page + cell_offset;
        
        struct table_interior_cell *curr = malloc(sizeof(struct table_interior_cell));
        if (i == 0) {
            head = curr;
            
        } else {
            prev->next = curr;
        }
        prev = curr;
        
        
        curr->type = 0xd;
        
        // payload size
        struct varint payload_size;
        decode_varint(cell, &payload_size);
        
        cell += payload_size.length_inbyte;
        
        // key
        struct varint key;
        decode_varint(cell, &key);
        
        cell += key.length_inbyte;
        
        // hdr size
        struct varint hdr_size;
        decode_varint(cell, &hdr_size);
        
        // to do....
        
        // update cell pointer
        cell_pointer += 2;
    }
    
    return head;
    
    free(page);
    return NULL;
}

struct master_header* read_master_header(void) {
    
    struct master_header * mheader= malloc(sizeof(struct master_header));
    
    if (read(db_fd, mheader, 100) != 100) {
        perror("read_master_header");
        exit(-1);
    }
    
    return mheader;
}

int btol16(uint16_t big){
    uint8_t* firstbyte = (uint8_t *)&big;
    
    uint16_t little = 0;
    little |= *firstbyte << 8;
    little |= *(firstbyte + 1);
    
    return little;
}

int btol24(uint32_t big){
    uint8_t* firstbyte = (uint8_t *)&big;
    uint8_t* secondbyte = (uint8_t *)&big + 1;
    uint8_t* thirdbyte = (uint8_t *)&big + 2;
    
    uint32_t little = 0;
    little |= *firstbyte << 16;
    little |= *secondbyte << 8;
    little |= *thirdbyte;
    
    return little;
}

int btol32(uint32_t big){
    uint8_t* firstbyte = (uint8_t *)&big;
    uint8_t* secondbyte = (uint8_t *)&big + 1;
    uint8_t* thirdbyte = (uint8_t *)&big + 2;
    uint8_t* forthbyte = (uint8_t *)&big + 3;
    
    uint32_t little = 0;
    little |= *firstbyte << 24;
    little |= *secondbyte << 16;
    little |= *thirdbyte << 8;
    little |= *forthbyte;
    
    return little;
}

// Only support decode varint up to 8 bytes
int decode_varint(uint8_t *start, struct varint *varint){
    long value = 0;
    int length = 0;
    char raw[8];
    for (int i = 0; i < 8; i++) {
        raw[i] = *(start + i) & 0b01111111;
        length++;
        // Once see 0 starting byte, reach end
        if ((*(start + i) & 0b10000000) == 0) {
            break;
        }
    }
    
    int shift = length - 1;
    for (int i = 0; i < 8; i++) {
        value |= raw[i] << (shift * 7);
        if (shift == 0) {
            break;
        }
        shift--;
    }
    
    varint->length_inbyte = length;
    varint->value = value;
    
    return 0;
}


