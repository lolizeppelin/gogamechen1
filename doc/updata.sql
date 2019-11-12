ALTER TABLE `groups` ADD `platfrom_id` mediumint(8) unsigned NOT NULL AFTER `name`;
ALTER TABLE `groups` ADD `warsvr` tinyint(1) NOT NULL AFTER `platfrom_id`;
ALTER TABLE `gameareas` ADD `gid` bigint(20) unsigned DEFAULT NULL AFTER `areaname`;
ALTER TABLE `appentitys` ADD `set_id` int(10) unsigned DEFAULT NULL AFTER `cross_id`;

ALTER TABLE `objtypefiles` ADD `srcname` varchar(256) NOT NULL AFTER `md5`;
ALTER TABLE `objtypefiles` ADD `group` int(10) unsigned NOT NULL AFTER `srcname`;


DROP TABLE IF EXISTS `warsvrsets`;
CREATE TABLE `warsvrsets` (
  `set_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `group_id` int(10) unsigned NOT NULL,
  `host` varchar(200) NOT NULL,
  `port` smallint(5) unsigned NOT NULL,
  `vhost` varchar(64) NOT NULL,
  `passwd` varchar(64) NOT NULL,
  PRIMARY KEY (`set_id`),
  UNIQUE KEY `vhost_unique` (`host`,`port`,`vhost`),
  KEY `group_index` (`group_id`),
  CONSTRAINT `warsvrsets_ibfk_1` FOREIGN KEY (`group_id`) REFERENCES `groups` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
