//
//  main.c
//  db_reader
//
//  Created by HUADONG XING on 2019-02-08.
//  Copyright © 2019 HUADONG XING. All rights reserved.
//

#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "database.h"
#include "db_utils.h"

#define NO_IDX 0
#define UNCLUST_IDX 1
#define CLUST_IDX 2

// Global Var
int db_mode;
int db_pg_size;
int db_fd;

// Counter
unsigned int scan_record_count;
unsigned int page_read_count;

int clust_rowe(int pg_num){
    char *target = "Rowe         ";
    
    struct node *node = (struct node *)read_page(pg_num);
    if (node->type_flag == 0x2) {
        struct clust_interior_cell *curr = (struct clust_interior_cell *)node;
        
        while (curr != NULL) {
            clust_rowe(curr->page_num);
            
            if (!strncmp(target, curr->record.Last_Name, 12)) {
                // display the info in record.
                printf("Employee ID: ");
                printf("%d", curr->record.Emp_ID);
                printf("  ");
                printf(curr->record.Name_Prefix);
                printf(curr->record.First_Name);
                printf(curr->record.Middle_Initial);
                printf(" ");
                printf(curr->record.Last_Name);
                printf("\n");
                
            }
            curr = curr->next;
        }
        
    }
    
    if (node->type_flag == 0xa) {
        struct clust_leaf_cell *curr = (struct clust_leaf_cell *)node;
        while (curr != NULL) {
            
            if (!strncmp(target, curr->record.Last_Name, 12)) {
                // display the info in record.
                printf("Employee ID: ");
                printf("%d", curr->record.Emp_ID);
                printf("  ");
                printf(curr->record.Name_Prefix);
                printf(curr->record.First_Name);
                printf(curr->record.Middle_Initial);
                printf(" ");
                printf(curr->record.Last_Name);
                printf("\n");
                
            }
            curr = curr->next;
        }
    }
    
    return 0;
}


// This is the recursion function to scan the database
// to find employee with last name "Rowe".
int scan_rowe(int pg_num){
    char *target = "Rowe         ";
    
    struct node *node = (struct node *)read_page(pg_num);
    
    // If this is an interior page, go down recursively
    if (node->type_flag == 0x5) {
        struct table_interior_cell *curr = (struct table_interior_cell *)node;
        while (curr != NULL) {
            scan_rowe(curr->page_num); // recursion
            // after scan, this page is useless, need to free it
            struct table_interior_cell *to_free = curr;
            curr = curr->next;
            free(to_free);
        }
        
        return 0;
    }
    
    // reach lead node, perform real scan
    if (node->type_flag == 0xd) {
        struct table_leaf_cell *curr = (struct table_leaf_cell *)node;
        while (curr != NULL) {
            if (!strncmp(target, curr->record.Last_Name, 12)) {
                printf("Employee ID: ");
                printf("%d", curr->record.Emp_ID);
                printf("  ");
                printf(curr->record.Name_Prefix);
                printf(curr->record.First_Name);
                printf(curr->record.Middle_Initial);
                printf(" ");
                printf(curr->record.Last_Name);
                printf("\n");

            }
            scan_record_count++;
            
            struct table_leaf_cell *to_free = curr;
            curr = curr->next;
            free(to_free);
        }
        
        return 0;
    }
    
    return 0;
}

// This is the recursion function to scan the database
// to find employee with Emp ID = 181162.
int scan_181162(int pg_num){
    int target = 181162;
    
    struct node *node = (struct node *)read_page(pg_num);
    
    // go down recursively
    if (node->type_flag == 0x5) {
        struct table_interior_cell *curr = (struct table_interior_cell *)node;
        while (curr != NULL) {
            scan_181162(curr->page_num); // recursion
            struct table_interior_cell *to_free = curr;
            curr = curr->next;
            free(to_free);
        }
        
        return 0;
    }
    
    // reach lead node, perform real scan
    if (node->type_flag == 0xd) {
        struct table_leaf_cell *curr = (struct table_leaf_cell *)node;
        while (curr != NULL) {
            if (curr->record.Emp_ID == target) {
                printf("Employee ID: ");
                printf("%d", curr->record.Emp_ID);
                printf("  ");
                printf(curr->record.Name_Prefix);
                printf(curr->record.First_Name);
                printf(curr->record.Middle_Initial);
                printf(" ");
                printf(curr->record.Last_Name);
                printf("\n");
                
            }
            scan_record_count++;
            struct table_leaf_cell *to_free = curr;
            curr = curr->next;
            free(to_free);
        }
        
        return 0;
    }
    
    return -1;
}

int scan_range(int pg_num, int start, int end){
    
    struct node *node = (struct node *)read_page(pg_num);
    
    // go down recursively
    if (node->type_flag == 0x5) {
        struct table_interior_cell *curr = (struct table_interior_cell *)node;
        while (curr != NULL) {
            scan_range(curr->page_num, start, end); // recursion
            struct table_interior_cell *to_free = curr;
            curr = curr->next;
            free(to_free);
        }
        
        return 0;
    }
    
    // reach lead node, perform real scan
    if (node->type_flag == 0xd) {
        struct table_leaf_cell *curr = (struct table_leaf_cell *)node;
        while (curr != NULL) {
            if ((curr->record.Emp_ID >= start) && (curr->record.Emp_ID <= end)) {
                printf("Employee ID: ");
                printf("%d", curr->record.Emp_ID);
                printf("  ");
                printf(curr->record.Name_Prefix);
                printf(curr->record.First_Name);
                printf(curr->record.Middle_Initial);
                printf(" ");
                printf(curr->record.Last_Name);
                printf("\n");
                
            }
            scan_record_count++;
            struct table_leaf_cell *to_free = curr;
            curr = curr->next;
            free(to_free);
        }
        
        return 0;
    }
    
    return -1;
}

int search_rowid_in_table(int pg_num, int rowid){
    struct node *node = (struct node *)read_page(pg_num);
    
    if (node->type_flag == 0x5) {
        struct table_interior_cell *curr = (struct table_interior_cell *)node;
        while (curr->key_rowid < rowid) {
            if (curr->next == NULL) {
                break;
            }
            curr = curr->next;
        }
        
        // if reach here, means rowid <= curr_rowid
        // then we need to go down recursively
        search_rowid_in_table(curr->page_num, rowid);
        return 0;
    }
    
    if (node->type_flag == 0xd) {
        struct table_leaf_cell *curr = (struct table_leaf_cell *)node;
        
        int not_found = 1;
        
        while (not_found) {
            if (curr == NULL) {
                printf("no match found.\n");
                exit(-1);
            }
            
            if (curr->rowid == rowid) {
                not_found = 0;
                printf("Employee ID: ");
                printf("%d", curr->record.Emp_ID);
                printf("  ");
                printf(curr->record.Name_Prefix);
                printf(curr->record.First_Name);
                printf(curr->record.Middle_Initial);
                printf(" ");
                printf(curr->record.Last_Name);
                printf("\n");
            }
            
            curr = curr->next;
        }
        
        return 0;
    }
    
    
    return 0;
}

int unclust_binary_empid(int pg_num, int emp_id) {
    struct node *node = (struct node *)read_page(pg_num);
    
    // if this is an interior page, go down recursively
    if (node->type_flag == 0x2) {
        struct index_interior_cell *curr = (struct index_interior_cell *)node;
        
        // check if target id > current node's emp id
        // if so go to the next node
        
        while (curr->key_emp_id < emp_id) {
            if (curr == NULL) {
                printf("not record found.\n");
                exit(-1);
            }
            curr = curr->next;
        }
        
        // if reach here, then target id must be less or equal
        // to emp id
        if (curr->key_emp_id == emp_id) {
            // if match found, search the rowid in table
            search_rowid_in_table(1, curr->rowid);
            return 0;
        } else {
            unclust_binary_empid(curr->page_num, emp_id);
        }
        
        return 0;
    }
    
    if (node->type_flag == 0xa) {
        struct index_lead_cell *curr = (struct index_leaf_cell *)node;
        
        while (curr->key_emp_id < emp_id) {
            if (curr->next == NULL) {
                printf("not record found.\n");
                exit(-1);
            }
            curr = curr->next;
        }
        
        if (curr->key_emp_id == emp_id) {
            // if match found, search the rowid in table
            search_rowid_in_table(1, curr->rowid);
            return 0;
        }
        
        return 0;
        
    }
    
    // if reach here no record founded
    printf("no match type.\n");
    exit(-1);
    return 0;
}

int unclust_range(int pg_num, int start, int end){
    struct node *node = (struct node *)read_page(pg_num);
    if (node->type_flag == 0x2) {
        struct index_interior_cell *curr = (struct index_interior_cell *)node;
        
        while (curr->key_emp_id < start) {
            if (curr == NULL) {
                printf("not record found.\n");
                exit(-1);
            }
            curr = curr->next;
        }
        
        if (curr->key_emp_id == start) {
            // if match found, search the rowid in table
            search_rowid_in_table(1, curr->rowid);
            return 0;
        } else {
            unclust_range(curr->page_num, start, end);
        }
        
    }
    
    if (node->type_flag == 0xa) {
        struct index_lead_cell *curr = (struct index_leaf_cell *)node;
        
        while (curr->key_emp_id < start) {
            curr = curr->next;
        }
        
        while ((curr->key_emp_id) >= start && (curr->key_emp_id <= end)) {
            search_rowid_in_table(1, curr->rowid);
            curr = curr->next;
        }
    }
    
    return 0;
}

int clust_empid(int pg_num, int target_id) {
    struct node *node = (struct node *)read_page(pg_num);
    
    if (node->type_flag == 0x2) {
        struct clust_interior_cell *curr = (struct clust_interior_cell *)node;
        
        while (curr->key_emp_id < target_id) {
            if (curr->next == NULL) {
                break;
            }
            curr = curr->next;
        }
        
        if (curr->key_emp_id == target_id) {
            // display the info in record.
            printf("Employee ID: ");
            printf("%d", curr->record.Emp_ID);
            printf("  ");
            printf(curr->record.Name_Prefix);
            printf(curr->record.First_Name);
            printf(curr->record.Middle_Initial);
            printf(" ");
            printf(curr->record.Last_Name);
            printf("\n");
            
        } else {
            clust_empid(curr->page_num, target_id);
        }
        
    }
    
    if (node->type_flag == 0xa) {
        struct clust_leaf_cell *curr = (struct clust_leaf_cell *)node;
        
        while (curr->key_emp_id < target_id) {
            if (curr == NULL) {
                printf("not record found.\n");
                exit(-1);
            }
            curr = curr->next;
        }
        
        if (curr->key_emp_id == target_id) {
            // display the info in record.
            printf("Employee ID: ");
            printf("%d", curr->record.Emp_ID);
            printf("  ");
            printf(curr->record.Name_Prefix);
            printf(curr->record.First_Name);
            printf(curr->record.Middle_Initial);
            printf(" ");
            printf(curr->record.Last_Name);
            printf("\n");
            
        }
    }
    
    
    
    return 0;
}

int clust_range(int pg_num, int start, int end) {
    struct node *node = (struct node *)read_page(pg_num);
    
    if (node->type_flag == 0x2) {
        struct clust_interior_cell *curr = (struct clust_interior_cell *)node;
        
        while (curr != NULL) {
            
            if (curr->key_emp_id < start) {
                // do nothing
            }
            
            if ((curr->key_emp_id >= start) && (curr->key_emp_id <= end)) {
                // print itself
                printf("Employee ID: ");
                printf("%d", curr->record.Emp_ID);
                printf("  ");
                printf(curr->record.Name_Prefix);
                printf(curr->record.First_Name);
                printf(curr->record.Middle_Initial);
                printf(" ");
                printf(curr->record.Last_Name);
                printf("\n");
                // recursive call
                clust_range(curr->page_num, start, end);
            }
            
            if (curr->key_emp_id > end) {
                // recursive call
                clust_range(curr->page_num, start, end);
                return 0;
            }
            
            curr = curr->next;
        }
        
        
    }
    
    if (node->type_flag == 0xa) {
        struct clust_leaf_cell *curr = (struct clust_leaf_cell *)node;
        
        while (curr != NULL) {
            
            if ((curr->key_emp_id >= start) && (curr->key_emp_id <= end)) {
                // print itself
                printf("Employee ID: ");
                printf("%d", curr->record.Emp_ID);
                printf("  ");
                printf(curr->record.Name_Prefix);
                printf(curr->record.First_Name);
                printf(curr->record.Middle_Initial);
                printf(" ");
                printf(curr->record.Last_Name);
                printf("\n");
            }
            
            curr = curr->next;
        }
        
        
    }
    
    return 0;
}

// To query and print the employee id and full name of anybody whose
// last name is "Rowe". Since there is no index on last name.
// So, we perform scan on all three db.
unsigned long query_one(){
    printf("_________query 1___________________\n");
    printf("--------result------\n");
    unsigned long prev_page_read_count = page_read_count;
    clock_t start;
    clock_t finish;
    
    // for the no idx db, we assume root page start at page 1
    if (db_mode == NO_IDX) {
        start = clock();
        scan_rowe(1);
        finish = clock();
    }
    
    if (db_mode ==  UNCLUST_IDX) {
        start = clock();
        scan_rowe(1);
        finish = clock();
    }
    
    if (db_mode == CLUST_IDX) {
        start = clock();
        clust_rowe(1);
        finish = clock();
    }
    printf("-------------");
    printf("Summery:\n");
    printf("page read count: %u\n", page_read_count - prev_page_read_count);
    double duration = (double)(finish - start)/CLOCKS_PER_SEC;
//    printf("CPU Cycle:%lu\n", finish - start);
    printf("Time used:%f\n", duration);
    printf("-----------------------------------\n");
    return finish - start;
}

// Query and print the employee id and full name of employee #181162.
unsigned long query_two(){
    printf("_________query 2___________________\n");
    printf("--------result------\n");
    unsigned long prev_page_read_count = page_read_count;
    clock_t start = 0;
    clock_t finish= 0;
    
    if (db_mode == NO_IDX) {
        // for the no idx db, we assume root page start at page 1
        start = clock();
        scan_181162(1);
        finish = clock();
    }
    
    if (db_mode == UNCLUST_IDX) {
        // for the unclust. db, we know the index start at 0xa798
        start = clock();
        unclust_binary_empid(0xa798, 181162);
        finish = clock();
    }
    
    if (db_mode == CLUST_IDX) {
        start = clock();
        clust_empid(1, 181162);
        finish = clock();
    }
    printf("-------------");
    printf("Summery:\n");
    printf("page read count: %u\n", page_read_count - prev_page_read_count);
    double duration = (double)(finish - start)/CLOCKS_PER_SEC;
//    printf("CPU Cycle:%lu\n", finish - start);
    printf("Time used:%f\n", duration);
    printf("-----------------------------------\n");
    return finish - start;
    
    return -1;
}

// Query and print the employee id and full name of employee with
// Emp ID between 171800 to 171899.
unsigned long query_three(){
    printf("_________query 3___________________\n");
    printf("--------result------\n");
    unsigned long prev_page_read_count = page_read_count;
    clock_t start = 0;
    clock_t finish= 0;
    
    if (db_mode == NO_IDX) {
        // for the no idx db, we assume root page start at page 1
        start = clock();
        scan_range(1, 171800, 171899);
        finish = clock();
    }
    
    if (db_mode == UNCLUST_IDX) {
        start = clock();
        unclust_range(0xa798, 171800, 171899);
        finish = clock();
    }
    
    if (db_mode == CLUST_IDX) {
        start = clock();
        clust_range(1, 171800, 171899);
        finish = clock();
    }
    printf("-------------");
    printf("Summery:\n");
    printf("page read count: %u\n", page_read_count - prev_page_read_count);
    double duration = (double)(finish - start)/CLOCKS_PER_SEC;
//    printf("CPU Cycle:%lu\n", finish - start);
    printf("Time used:%f\n", duration);
    printf("-----------------------------------\n");
    return finish - start;
    
    return -1;
}

int main(int argc, const char * argv[]) {
    
    if (argc != 3) {
        fprintf(stderr, "Usage: <DB file> <Mode Char>\n");
        fprintf(stderr, "\t\t\t\t  [N]o idx\n\t\t\t\t  [U]nclust. idx\n\t\t\t\t  [C]lust. idx\n");
        return -1;
    }
    
    // check mode
    char mode_char = argv[2][0];
    if (mode_char == 'N') {
        db_mode = NO_IDX;
        printf("Read DB in No index mode.\n");
    } else if (mode_char == 'U') {
        db_mode = UNCLUST_IDX;
        printf("Read DB in Unclustered Index Mode.\n");
    } else if (mode_char == 'C') {
        db_mode = CLUST_IDX;
        printf("Read DB in Clustered Index Mode.\n");
    } else {
        fprintf(stderr, "Incorrect mode char.\n");
        fprintf(stderr, "Usage: <DB file> <Mode>\n");
        fprintf(stderr, "\t<Mode> N: [N]o idx\n\t<Mode> U: [U]nclust. idx\n\t<Mode> C: [C]lust. idx\n");
    }
    
    // Init value
    scan_record_count = 0;
    page_read_count = 0;
    
    // open the db file
    db_fd = open(argv[1], O_RDONLY);
    if (db_fd == -1) {
        perror("Open db");
        printf("Open db file fail.\n");
        return -1;
    }
    
    struct master_header * mh = read_master_header();
    // read database page size
    db_pg_size = btol16(mh->pg_size);
    
    printf("Database Page Size = %d\n", db_pg_size);
    
    
    unsigned long cpu_usage_one = query_one();
    unsigned long cpu_usage_two = query_two();
    unsigned long cpu_usage_three = query_three();
    
    unsigned long cpu_total = cpu_usage_one + cpu_usage_two + cpu_usage_three;
    
    double duration = (double)cpu_total/CLOCKS_PER_SEC;
    
    printf("-----------------------------------\n");
//    printf("Total CPU Cycle:%lu\n", cpu_total);
    printf("Total Time Used:%f\n", duration);
    printf("Total Page Read:%u\n", page_read_count);
    
    printf("Avg. time/page:%f\n", duration/(double) page_read_count);
    printf("-----------------------------------\n");
    
    return 0;
}
