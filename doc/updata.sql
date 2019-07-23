alter TABLE `appentitys` add column `set_id` int(10) unsigned DEFAULT NULL after `cross_id`;
alter TABLE `gameareas` add column `gid` bigint(20) unsigned DEFAULT NULL after `areaname`;


alter TABLE `objtypefiles` add column `srcname` varchar(256) DEFAULT NULL after `md5`;
update `objtypefiles` set `srcname` = 'test.zip';
alter TABLE `objtypefiles` modify column `srcname` varchar(256) NOT NULL;

alter TABLE `objtypefiles` add column `group` int(10) unsigned DEFAULT NULL after `srcname`;
update `objtypefiles` set `group` = 0;
alter TABLE `objtypefiles` modify column `group` int(10) unsigned NOT NULL;


alter TABLE `groups` add column `platfrom_id` mediumint(8) unsigned DEFAULT NULL after `name`;
update `groups` set `platfrom_id` = 0;
alter TABLE `groups` modify column `platfrom_id` mediumint(8) unsigned NOT NULL;


alter TABLE `groups` add column warsvr` tinyint(1) DEFAULT NULL after `platfrom_id`;
update `groups` set `warsvr` = 0;
alter TABLE `groups` modify column warsvr` tinyint(1) NOT NULL;