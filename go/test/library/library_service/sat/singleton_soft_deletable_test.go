package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func getAuthorProfile(t *testing.T, name string) *librarypb.AuthorProfile {
	t.Helper()
	getAuthorProfileRequest := &libraryservicepb.GetAuthorProfileRequest{Name: name}
	authorProfile, err := libraryServiceClient.GetAuthorProfile(ctx, getAuthorProfileRequest)
	require.NoError(t, err)
	return authorProfile
}

func TestAuthorProfile_AutoCreatedWithAuthor(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Profile Auto Author")

	profileName := author.Name + "/profile"
	profile := getAuthorProfile(t, profileName)
	require.Equal(t, profileName, profile.Name)
	require.NotNil(t, profile.CreateTime)
	require.NotNil(t, profile.UpdateTime)
	require.Equal(t, author.CreateTime.AsTime(), profile.CreateTime.AsTime())
	require.Nil(t, profile.DeleteTime)
	require.NotEmpty(t, profile.Etag)
	require.Empty(t, profile.Bio)
	require.Empty(t, profile.Website)
}

func TestAuthorProfile_AutoCreatedWithAuthor_IdempotentCreation(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
		Parent: organizationParent,
		Author: &librarypb.Author{
			DisplayName:    "Idempotent Profile Author",
			EmailAddress:   "idempotent-profile@test.com",
			EmailAddresses: []string{"mytest@gmail.com"},
			Metadata:       &librarypb.AuthorMetadata{},
		},
		RequestId: "a1b2c3d4-e5f6-7890-abcd-ef1234567001",
	}

	author1, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
	require.NoError(t, err)

	author2, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
	require.NoError(t, err)
	require.Equal(t, author1.Name, author2.Name)

	profile := getAuthorProfile(t, author1.Name+"/profile")
	require.Equal(t, author1.Name+"/profile", profile.Name)
	require.Equal(t, author1.CreateTime.AsTime(), profile.CreateTime.AsTime())
}

func TestAuthorProfile_JoinFieldsPopulated(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Profile Join Author")

	profile := getAuthorProfile(t, author.Name+"/profile")
	require.Equal(t, "Profile Join Author", profile.AuthorDisplayName)
	require.Equal(t, author.EmailAddress, profile.AuthorEmailAddress)
}

func TestAuthorProfile_JoinFieldsReflectParentUpdate(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Original Name")

	profile := getAuthorProfile(t, author.Name+"/profile")
	require.Equal(t, "Original Name", profile.AuthorDisplayName)

	updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
		Author: &librarypb.Author{
			Name:        author.Name,
			DisplayName: "Updated Name",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
	require.NoError(t, err)

	profile = getAuthorProfile(t, author.Name+"/profile")
	require.Equal(t, "Updated Name", profile.AuthorDisplayName)
}

func TestAuthorProfile_Update(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Profile Update Author")

	t.Run("UpdateBioAndWebsite", func(t *testing.T) {
		t.Parallel()
		profileName := author.Name + "/profile"
		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name:    profileName,
				Bio:     "A prolific writer.",
				Website: "https://example.com",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio", "website"}},
		}
		updatedProfile, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		require.NoError(t, err)
		require.Equal(t, "A prolific writer.", updatedProfile.Bio)
		require.Equal(t, "https://example.com", updatedProfile.Website)

		gotProfile := getAuthorProfile(t, profileName)
		grpcrequire.Equal(t, updatedProfile, gotProfile)
	})

	t.Run("UpdateLabels", func(t *testing.T) {
		t.Parallel()
		secondAuthor := createTestAuthor(t, organizationParent, "Profile Labels Author")
		profileName := secondAuthor.Name + "/profile"
		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name:   profileName,
				Labels: map[string]string{"featured": "true"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		}
		updatedProfile, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		require.NoError(t, err)
		require.Equal(t, "true", updatedProfile.Labels["featured"])
	})

	t.Run("UpdateMetadata", func(t *testing.T) {
		t.Parallel()
		thirdAuthor := createTestAuthor(t, organizationParent, "Profile Meta Author")
		profileName := thirdAuthor.Name + "/profile"
		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
				Metadata: &librarypb.AuthorProfileMetadata{
					PreferredLanguage: "en",
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata"}},
		}
		updatedProfile, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		require.NoError(t, err)
		require.Equal(t, "en", updatedProfile.Metadata.PreferredLanguage)
	})

	t.Run("EtagChanges", func(t *testing.T) {
		t.Parallel()
		fourthAuthor := createTestAuthor(t, organizationParent, "Profile Etag Author")
		profileName := fourthAuthor.Name + "/profile"
		original := getAuthorProfile(t, profileName)
		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
				Bio:  "Etag test bio.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
		}
		updatedProfile, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		require.NoError(t, err)
		require.NotEqual(t, original.Etag, updatedProfile.Etag)
	})

	t.Run("UnauthorizedField_Name", func(t *testing.T) {
		t.Parallel()
		fifthAuthor := createTestAuthor(t, organizationParent, "Profile Unauth Author")
		profileName := fifthAuthor.Name + "/profile"
		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		}
		_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("EmptyUpdateMask", func(t *testing.T) {
		t.Parallel()
		sixthAuthor := createTestAuthor(t, organizationParent, "Profile EmptyMask Author")
		profileName := sixthAuthor.Name + "/profile"
		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
				Bio:  "Should fail",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
		}
		_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("PartialUpdatePreservesOtherFields", func(t *testing.T) {
		t.Parallel()
		seventhAuthor := createTestAuthor(t, organizationParent, "Profile Partial Author")
		profileName := seventhAuthor.Name + "/profile"

		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name:    profileName,
				Bio:     "Original bio",
				Website: "https://original.com",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio", "website"}},
		}
		_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		require.NoError(t, err)

		updateAuthorProfileRequest = &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
				Bio:  "Updated bio",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
		}
		updatedProfile, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		require.NoError(t, err)
		require.Equal(t, "Updated bio", updatedProfile.Bio)
		require.Equal(t, "https://original.com", updatedProfile.Website)
	})

	t.Run("EtagMismatch_WithExplicitEtag", func(t *testing.T) {
		t.Parallel()
		eighthAuthor := createTestAuthor(t, organizationParent, "Profile EtagMis Author")
		profileName := eighthAuthor.Name + "/profile"
		original := getAuthorProfile(t, profileName)

		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
				Bio:  "First update",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
		}
		_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		require.NoError(t, err)

		updateAuthorProfileRequest = &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
				Etag: original.Etag,
				Bio:  "Stale update",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
		}
		_, err = libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("EtagMismatch_WithoutExplicitEtag_Succeeds", func(t *testing.T) {
		t.Parallel()
		ninthAuthor := createTestAuthor(t, organizationParent, "Profile NoEtag Author")
		profileName := ninthAuthor.Name + "/profile"

		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
				Bio:  "No etag update",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
		}
		updatedProfile, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		require.NoError(t, err)
		require.Equal(t, "No etag update", updatedProfile.Bio)
	})

	t.Run("UnauthorizedField_CreateTime", func(t *testing.T) {
		t.Parallel()
		tenthAuthor := createTestAuthor(t, organizationParent, "Profile CT Author")
		profileName := tenthAuthor.Name + "/profile"
		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"create_time"}},
		}
		_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UnauthorizedField_DeleteTime", func(t *testing.T) {
		t.Parallel()
		eleventhAuthor := createTestAuthor(t, organizationParent, "Profile DT Author")
		profileName := eleventhAuthor.Name + "/profile"
		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"delete_time"}},
		}
		_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UnauthorizedField_Etag", func(t *testing.T) {
		t.Parallel()
		twelfthAuthor := createTestAuthor(t, organizationParent, "Profile EtagField Author")
		profileName := twelfthAuthor.Name + "/profile"
		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"etag"}},
		}
		_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UnauthorizedField_JoinField", func(t *testing.T) {
		t.Parallel()
		thirteenthAuthor := createTestAuthor(t, organizationParent, "Profile JoinField Author")
		profileName := thirteenthAuthor.Name + "/profile"
		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"author_display_name"}},
		}
		_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UpdateTimeAdvances", func(t *testing.T) {
		t.Parallel()
		fourteenthAuthor := createTestAuthor(t, organizationParent, "Profile UT Author")
		profileName := fourteenthAuthor.Name + "/profile"
		original := getAuthorProfile(t, profileName)

		updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
			AuthorProfile: &librarypb.AuthorProfile{
				Name: profileName,
				Bio:  "Time check",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
		}
		updatedProfile, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
		require.NoError(t, err)
		require.True(t, updatedProfile.UpdateTime.AsTime().After(original.UpdateTime.AsTime()) ||
			updatedProfile.UpdateTime.AsTime().Equal(original.UpdateTime.AsTime()))
		require.Equal(t, original.CreateTime.AsTime(), updatedProfile.CreateTime.AsTime())
	})
}

func TestAuthorProfile_JoinFieldsOnUpdate(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Profile JoinOnUpd Author")
	profileName := author.Name + "/profile"

	updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
		AuthorProfile: &librarypb.AuthorProfile{
			Name: profileName,
			Bio:  "Updated bio",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
	}
	updatedProfile, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
	require.NoError(t, err)
	require.Equal(t, "Profile JoinOnUpd Author", updatedProfile.AuthorDisplayName)
	require.Equal(t, "Updated bio", updatedProfile.Bio)
}

func TestAuthorProfile_GetNotFound(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	getAuthorProfileRequest := &libraryservicepb.GetAuthorProfileRequest{
		Name: organizationParent + "/authors/nonexistent-author/profile",
	}
	_, err := libraryServiceClient.GetAuthorProfile(ctx, getAuthorProfileRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestAuthorProfile_GetWithWildcard(t *testing.T) {
	t.Parallel()
	getAuthorProfileRequest := &libraryservicepb.GetAuthorProfileRequest{
		Name: "organizations/-/authors/-/profile",
	}
	_, err := libraryServiceClient.GetAuthorProfile(ctx, getAuthorProfileRequest)
	grpcrequire.Error(t, codes.InvalidArgument, err)
}

func TestAuthorProfile_DeletedWithParent(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Profile Delete Author")
	profileName := author.Name + "/profile"

	updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
		AuthorProfile: &librarypb.AuthorProfile{
			Name: profileName,
			Bio:  "Soon to be deleted.",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
	}
	_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
	require.NoError(t, err)

	deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
	deletedAuthor, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
	require.NoError(t, err)
	require.NotNil(t, deletedAuthor.DeleteTime)

	deletedAuthorProfile, err := libraryServiceClient.GetAuthorProfile(ctx, &libraryservicepb.GetAuthorProfileRequest{Name: profileName})
	require.NoError(t, err)
	require.NotNil(t, deletedAuthorProfile.GetDeleteTime())
}

func TestAuthorProfile_DeletedWithParent_ThenUpdate_Fails(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Profile DelUpd Author")
	profileName := author.Name + "/profile"

	deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
	_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
	require.NoError(t, err)

	updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
		AuthorProfile: &librarypb.AuthorProfile{
			Name: profileName,
			Bio:  "Should fail",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
	}
	_, err = libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestAuthorProfile_DeletedWithParent_AllowMissing(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Profile DelAllow Author")

	deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
		Name:         author.Name,
		AllowMissing: false,
	}
	_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
	require.NoError(t, err)

	deleteAuthorRequest = &libraryservicepb.DeleteAuthorRequest{
		Name:         author.Name,
		AllowMissing: true,
	}
	deletedAuthor, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
	require.NoError(t, err)
	require.NotNil(t, deletedAuthor.DeleteTime)
}

func TestAuthorProfile_MultipleAuthors_IndependentProfiles(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	authorA := createTestAuthor(t, organizationParent, "Author A Profile")
	authorB := createTestAuthor(t, organizationParent, "Author B Profile")

	updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
		AuthorProfile: &librarypb.AuthorProfile{
			Name: authorA.Name + "/profile",
			Bio:  "Bio A",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
	}
	_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
	require.NoError(t, err)

	profileA := getAuthorProfile(t, authorA.Name+"/profile")
	profileB := getAuthorProfile(t, authorB.Name+"/profile")

	require.Equal(t, "Bio A", profileA.Bio)
	require.Empty(t, profileB.Bio)
	require.Equal(t, "Author A Profile", profileA.AuthorDisplayName)
	require.Equal(t, "Author B Profile", profileB.AuthorDisplayName)
}

func TestAuthorProfile_List(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Profile List Author")

	updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
		AuthorProfile: &librarypb.AuthorProfile{
			Name: author.Name + "/profile",
			Bio:  "Listed bio",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
	}
	_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
	require.NoError(t, err)

	listAuthorProfilesRequest := &libraryservicepb.ListAuthorProfilesRequest{
		Parent: author.Name,
	}
	listAuthorProfilesResponse, err := libraryServiceClient.ListAuthorProfiles(ctx, listAuthorProfilesRequest)
	require.NoError(t, err)
	require.Len(t, listAuthorProfilesResponse.AuthorProfiles, 1)
	require.Equal(t, author.Name+"/profile", listAuthorProfilesResponse.AuthorProfiles[0].Name)
	require.Equal(t, "Listed bio", listAuthorProfilesResponse.AuthorProfiles[0].Bio)
	require.Equal(t, "Profile List Author", listAuthorProfilesResponse.AuthorProfiles[0].AuthorDisplayName)
}

func TestAuthorProfile_List_ExcludesSoftDeleted(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Profile ListExcl Author")

	listAuthorProfilesRequest := &libraryservicepb.ListAuthorProfilesRequest{
		Parent: author.Name,
	}
	listAuthorProfilesResponse, err := libraryServiceClient.ListAuthorProfiles(ctx, listAuthorProfilesRequest)
	require.NoError(t, err)
	require.Len(t, listAuthorProfilesResponse.AuthorProfiles, 1)

	deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
	_, err = libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
	require.NoError(t, err)

	listAuthorProfilesResponse, err = libraryServiceClient.ListAuthorProfiles(ctx, listAuthorProfilesRequest)
	require.NoError(t, err)
	require.Empty(t, listAuthorProfilesResponse.AuthorProfiles)
}

func TestAuthorProfile_List_ShowDeletedIncludesSoftDeleted(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Profile ListShow Author")

	deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
	_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
	require.NoError(t, err)

	listAuthorProfilesRequest := &libraryservicepb.ListAuthorProfilesRequest{
		Parent:      author.Name,
		ShowDeleted: true,
	}
	listAuthorProfilesResponse, err := libraryServiceClient.ListAuthorProfiles(ctx, listAuthorProfilesRequest)
	require.NoError(t, err)
	require.Len(t, listAuthorProfilesResponse.AuthorProfiles, 1)
	require.NotNil(t, listAuthorProfilesResponse.AuthorProfiles[0].DeleteTime)
}

func TestAuthorProfile_BatchGet(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	parent := organizationParent + "/authors/-"
	authorA := createTestAuthor(t, organizationParent, "Profile BatchGet A")
	authorB := createTestAuthor(t, organizationParent, "Profile BatchGet B")
	profileNameA := authorA.Name + "/profile"
	profileNameB := authorB.Name + "/profile"

	updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
		AuthorProfile: &librarypb.AuthorProfile{
			Name: profileNameA,
			Bio:  "Bio A",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
	}
	_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorProfilesRequest := &libraryservicepb.BatchGetAuthorProfilesRequest{
			Parent: parent,
			Names:  []string{profileNameA, profileNameB},
		}
		batchGetAuthorProfilesResponse, err := libraryServiceClient.BatchGetAuthorProfiles(ctx, batchGetAuthorProfilesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetAuthorProfilesResponse.AuthorProfiles, 2)

		profileNameToAuthorProfile := map[string]*librarypb.AuthorProfile{}
		for _, profile := range batchGetAuthorProfilesResponse.AuthorProfiles {
			profileNameToAuthorProfile[profile.Name] = profile
		}
		require.Equal(t, "Bio A", profileNameToAuthorProfile[profileNameA].Bio)
		require.Empty(t, profileNameToAuthorProfile[profileNameB].Bio)
		require.Equal(t, "Profile BatchGet A", profileNameToAuthorProfile[profileNameA].AuthorDisplayName)
		require.Equal(t, "Profile BatchGet B", profileNameToAuthorProfile[profileNameB].AuthorDisplayName)
	})

	t.Run("PreservesRequestOrder", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorProfilesRequest := &libraryservicepb.BatchGetAuthorProfilesRequest{
			Parent: parent,
			Names:  []string{profileNameB, profileNameA},
		}
		batchGetAuthorProfilesResponse, err := libraryServiceClient.BatchGetAuthorProfiles(ctx, batchGetAuthorProfilesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetAuthorProfilesResponse.AuthorProfiles, 2)
		require.Equal(t, profileNameB, batchGetAuthorProfilesResponse.AuthorProfiles[0].Name)
		require.Equal(t, profileNameA, batchGetAuthorProfilesResponse.AuthorProfiles[1].Name)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorProfilesRequest := &libraryservicepb.BatchGetAuthorProfilesRequest{
			Parent: parent,
			Names:  []string{profileNameA, organizationParent + "/authors/nonexistent-author/profile"},
		}
		_, err := libraryServiceClient.BatchGetAuthorProfiles(ctx, batchGetAuthorProfilesRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestAuthorProfile_Update_ClearableFields(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Profile Clear Author")
	profileName := author.Name + "/profile"

	updateAuthorProfileRequest := &libraryservicepb.UpdateAuthorProfileRequest{
		AuthorProfile: &librarypb.AuthorProfile{
			Name:    profileName,
			Bio:     "Will be cleared",
			Website: "https://will-be-cleared.com",
			Labels:  map[string]string{"key": "value"},
			Metadata: &librarypb.AuthorProfileMetadata{
				PreferredLanguage: "fr",
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio", "website", "labels", "metadata"}},
	}
	_, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
	require.NoError(t, err)

	updateAuthorProfileRequest = &libraryservicepb.UpdateAuthorProfileRequest{
		AuthorProfile: &librarypb.AuthorProfile{
			Name:     profileName,
			Bio:      "",
			Website:  "",
			Labels:   nil,
			Metadata: nil,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio", "website", "labels", "metadata"}},
	}
	updatedProfile, err := libraryServiceClient.UpdateAuthorProfile(ctx, updateAuthorProfileRequest)
	require.NoError(t, err)
	require.Empty(t, updatedProfile.Bio)
	require.Empty(t, updatedProfile.Website)
	require.Empty(t, updatedProfile.Labels)
	require.Nil(t, updatedProfile.Metadata)
}
