
package models

import "go.mongodb.org/mongo-driver/bson/primitive"

type Object struct {
	ID        primitive.ObjectID `bson:"_id"`
	Name      string             `bson:"name"`
	TenantId  primitive.ObjectID `bson:"tenantId"`
	ParentId  primitive.ObjectID `bson:"parentId"`
	IsDeleted bool               `bson:"isDeleted"`
	Type      string             `bson:"type"`
	CreatedBy struct {
		Oid   string `bson:"oid"`
		Name  string `bson:"name"`
		Email string `bson:"email"`
	} `bson:"createdBy"`
	ModifiedBy struct {
		Oid   string `bson:"oid"`
		Name  string `bson:"name"`
		Email string `bson:"email"`
	} `bson:"modifiedBy"`
	CreatedDate  primitive.DateTime `bson:"createdDate"`
	ModifiedDate primitive.DateTime `bson:"modifiedDate"`
}

type ObjectType struct {
	Id   primitive.ObjectID
	Type string
}

