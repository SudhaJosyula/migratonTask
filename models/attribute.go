package models

import "go.mongodb.org/mongo-driver/bson/primitive"

type Attribute struct {
	ID          primitive.ObjectID `bson:"_id"`
	Name        string             `bson:"name"`
	TenantId    primitive.ObjectID `bson:"tenantId"`
	ParentId    primitive.ObjectID `bson:"parentId"`
	IsDeleted   bool               `bson:"isDeleted"`
	Type        string             `bson:"type"`
	DataType    string             `bson:"dataType"`
	Value       interface{}        `bson:"value"`
	ParentType  string             `bson:"parentType"`
	AttributeId primitive.ObjectID `bson:"attributeId"`
	CategoryId  primitive.ObjectID `bson:"categoryId"`
	CreatedBy   struct {
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
	PartitionKey string             `bson:"partitionKey"`
}