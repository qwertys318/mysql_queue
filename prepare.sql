CREATE DATABASE queue;
USE queue;

DELIMITER //

CREATE PROCEDURE sp_create_queue (queue_name VARCHAR(50))
DETERMINISTIC
BEGIN
    SET @stm = CONCAT('
        CREATE TABLE `queue_', queue_name, '` (
            `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
            `data` TEXT NOT NULL,
            `connection_id` BIGINT(21) UNSIGNED NULL DEFAULT NULL,
            `attempts_num` TINYINT(3) UNSIGNED NOT NULL DEFAULT 0,
            `exec_ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `created_ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE INDEX `connection_id` (`connection_id`),
            INDEX `attempts_num` (`attempts_num`),
            INDEX `exec_ts` (`exec_ts`)
        )COLLATE="utf8_general_ci" ENGINE=InnoDB
    ');
    PREPARE stm FROM @stm;
    EXECUTE stm;
    DEALLOCATE PREPARE stm;

    SET @stm = CONCAT('
        CREATE TABLE `queue_', queue_name, '_dlq` (
            `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
            `data` TEXT NOT NULL,
            `created_ts` TIMESTAMP NOT NULL,
            PRIMARY KEY (`id`)
        )COLLATE="utf8_general_ci" ENGINE=InnoDB
    ');
    PREPARE stm FROM @stm;
    EXECUTE stm;
    DEALLOCATE PREPARE stm;
END //

CREATE  PROCEDURE sp_get_message (queue_name VARCHAR(50))
DETERMINISTIC
BEGIN
    SET @stm = CONCAT('
        DELETE FROM queue_', queue_name, '
        WHERE connection_id = ', CONNECTION_ID()
    );
    PREPARE stm FROM @stm;
    EXECUTE stm;
    DEALLOCATE PREPARE stm;

    SET @stm = CONCAT('
        UPDATE queue_', queue_name, '
        SET
            connection_id = NULL,
            exec_ts = ADDDATE(NOW(), INTERVAL attempts_num * 10 + 1 MINUTE),
            attempts_num = attempts_num + 1
        WHERE connection_id NOT IN(
            SELECT ID
            FROM information_schema.PROCESSLIST
            WHERE DB = "queue"
        )
    ');
    PREPARE stm FROM @stm;
    EXECUTE stm;
    DEALLOCATE PREPARE stm;

    -- @TODO MULTITHREADING
    SET @stm = CONCAT('
        INSERT INTO queue_', queue_name, '_dlq
            (data, created_ts)
            SELECT data, created_ts
            FROM queue_', queue_name, '
            WHERE attempts_num = 4
    ');
    PREPARE stm FROM @stm;
    EXECUTE stm;
    DEALLOCATE PREPARE stm;

    SET @stm = CONCAT('
        DELETE FROM queue_', queue_name, '
        WHERE attempts_num = 4
    ');
    PREPARE stm FROM @stm;
    EXECUTE stm;
    DEALLOCATE PREPARE stm;

    SET @i = 0;
    waitmessage: LOOP
        SET @i = @i + 1;
        IF @i = 120 THEN
            LEAVE waitmessage;
        END IF ;
        -- @TODO PREPARE STM BEFORE LOOP?
        SET @stm = CONCAT('
            UPDATE queue_', queue_name, '
            SET connection_id = ', CONNECTION_ID(), '
            WHERE
                connection_id IS NULL
                AND exec_ts < NOW()
            ORDER BY id ASC
            LIMIT 1
        ');
        PREPARE stm FROM @stm;
        EXECUTE stm;
        IF ROW_COUNT() = 1 THEN
            SET @stm = CONCAT('
                SELECT data
                FROM queue_', queue_name, '
                WHERE connection_id = ', CONNECTION_ID()
            );
            PREPARE stm FROM @stm;
            EXECUTE stm;
            DEALLOCATE PREPARE stm;
            LEAVE waitmessage;
        END IF;
        DEALLOCATE PREPARE stm;
        DO SLEEP(0.5);
    END LOOP waitmessage;
END //

CREATE  PROCEDURE sp_create_message (queue_name VARCHAR(50), data TEXT)
DETERMINISTIC
BEGIN
    SET @stm = CONCAT('
        INSERT INTO queue_', queue_name, '
            (data)
        VALUES
            (?)
    ');
    PREPARE stm FROM @stm;
    EXECUTE stm USING data;
    DEALLOCATE PREPARE stm;
END //

DELIMITER ;
