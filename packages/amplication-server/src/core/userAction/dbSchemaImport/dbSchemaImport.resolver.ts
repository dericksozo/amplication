import { GqlAuthGuard } from "../../../guards/gql-auth.guard";
import { UseFilters, UseGuards } from "@nestjs/common";
import { Args, Mutation, Resolver } from "@nestjs/graphql";
import { FileUpload, GraphQLUpload } from "graphql-upload";
import { GqlResolverExceptionsFilter } from "../../../filters/GqlResolverExceptions.filter";
import { AuthorizeContext } from "../../../decorators/authorizeContext.decorator";
import { AuthorizableOriginParameter } from "../../../enums/AuthorizableOriginParameter";
import { User } from "../../../models";
import { UserEntity } from "../../../decorators/user.decorator";
import { DBSchemaImportService } from "./dbSchemaImport.service";
import { graphqlUpload } from "../utils/graphql-upload";
import { UserAction } from "../dto";
import { DBSchemaImportMetadata } from "./types";
import { CreateDBSchemaImportArgs } from "./dto/CreateDBSchemaImportArgs";

@Resolver(() => UserAction)
@UseFilters(GqlResolverExceptionsFilter)
@UseGuards(GqlAuthGuard)
export class DBSchemaImportResolver {
  constructor(private readonly dbSchemaImportService: DBSchemaImportService) {}

  @Mutation(() => UserAction, {
    nullable: false,
  })
  @AuthorizeContext(
    AuthorizableOriginParameter.ResourceId,
    "data.resource.connect.id"
  )
  async createEntitiesFromPrismaSchema(
    @UserEntity() user: User,
    @Args() args: CreateDBSchemaImportArgs,
    @Args({ name: "file", type: () => GraphQLUpload })
    file: FileUpload
  ): Promise<UserAction> {
    const fileContent = await graphqlUpload(file);
    const metadata: DBSchemaImportMetadata = {
      fileName: file.filename,
      schema: fileContent,
    };

    return this.dbSchemaImportService.startProcessingDBSchema(
      args,
      metadata,
      user
    );
  }
}