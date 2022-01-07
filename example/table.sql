CREATE TABLE `es_resource` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL COMMENT 'resource name',
  `description` varchar(100) NOT NULL COMMENT 'resource description',
  `create_time` bigint(20) NOT NULL COMMENT 'create_time',
  `update_time` bigint(20) NOT NULL COMMENT 'update_time',
  `delete_time` bigint(20) DEFAULT '0' COMMENT 'delete_time',
  PRIMARY KEY (`id`),
  KEY `update_time` (`update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `es_resource_role` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `resource_id` int(11) NOT NULL,
  `role_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_resource` (`user_id`,`resource_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;