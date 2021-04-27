﻿using NPoco;
using Umbraco.Core.Persistence.DatabaseAnnotations;

namespace Umbraco.Core.Persistence.Dtos
{
    [TableName(Constants.DatabaseSchema.Tables.UserGroup2NodePermission)]
    [ExplicitColumns]
    [PrimaryKey("userGroupId, nodeId, permission", AutoIncrement = false)]
    internal class UserGroup2NodePermissionDto
    {
        [Column("userGroupId")]
        [PrimaryKeyColumn(AutoIncrement = false, Name = "PK_umbracoUserGroup2NodePermission", OnColumns = "userGroupId, nodeId, permission")]
        [ForeignKey(typeof(UserGroupDto))]
        public int UserGroupId { get; set; }

        [Column("nodeId")]
        [ForeignKey(typeof(NodeDto))]
        [Index(IndexTypes.NonClustered, Name = "IX_umbracoUser2NodePermission_nodeId")]
        public int NodeId { get; set; }

        [Column("permission")]
        public string Permission { get; set; }
    }
}
