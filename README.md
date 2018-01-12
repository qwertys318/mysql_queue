# mysql_queue

## Preparation
Execute by root prepare.sql  
  
Create queue:  
`CALL sp_create_queue('QUEUE_NAME');`

Create user, grant privileges:
```
CREATE USER consumer@localhost IDENTIFIED BY 'PASSWORD';
GRANT EXECUTE ON queue.* TO consumer@localhost;
FLUSH PRIVILEGES;
```

## Using
Create message:  
`CALL sp_create_message('QUEUE_NAME', 'QUEUE_MESSAGE');`
  
Consuming:  
`CALL sp_get_message('QUEUE_NAME');`  
Enclose in `while(true)`
