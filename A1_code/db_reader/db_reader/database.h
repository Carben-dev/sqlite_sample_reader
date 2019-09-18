//
//  database.h
//  db_reader
//
//  Created by HUADONG XING on 2019-02-08.
//  Copyright © 2019 HUADONG XING. All rights reserved.
//

#ifndef database_h
#define database_h

/*****************************
 *        Record struct      *
 *****************************/
struct employee_record {
    int Emp_ID;
    char Name_Prefix[6];
    char First_Name[12];
    char Middle_Initial[2];
    char Last_Name[14];
};

/*****************************
 * Higher level tree struct. *
 *****************************/

// This is a link list sqlite master table
struct sqlite_master {
    char type;
    char *name;
    char *tbl_name;
    int rootpage;
    char *sql;
    struct sqlite_master *next;
};

struct table_leaf_cell {
    char type;
    long payload_size;
    long rowid;
    struct employee_record record;
    struct table_leaf_cell *next;
};

struct table_interior_cell {
    char type;
    int page_num;
    long key_rowid;
    struct table_interior_cell *next;
};

struct index_lead_cell {
    char type;
    int key_emp_id;
    int rowid;
    struct index_lead_cell *next;
};

struct index_interior_cell {
    char type;
    int page_num;
    int key_emp_id;
    int rowid;
    struct index_interior_cell *next;
    
};

struct clust_interior_cell {
    char type;
    int page_num;
    int key_emp_id;
    struct employee_record record;
    struct clust_interior_cell * next;
};

struct clust_leaf_cell {
    char type;
    int key_emp_id;
    struct employee_record record;
    struct clust_leaf_cell * next;
};

/*****************************
 * Lower level page struct.  *
 *****************************/

struct node {
    uint8_t type_flag;
} __attribute__ ((packed)) ;

// This is the first 100 bytes
struct master_header {
    char sqlite_string[16];
    uint16_t pg_size;
    char not_useful[82];
} __attribute__ ((packed)) ;

// header for all type b_tree
struct b_tree_header {
    uint8_t type_flag;
    uint16_t first_freeblock;
    uint16_t cell_count;
    uint16_t cell_start_off;
    uint8_t fragm_freebytes;
} __attribute__ ((packed)) ;

/*****************************
 *       SQLite Varint       *
 *****************************/
struct varint {
    long value;
    int length_inbyte;
};

#endif /* database_h */
