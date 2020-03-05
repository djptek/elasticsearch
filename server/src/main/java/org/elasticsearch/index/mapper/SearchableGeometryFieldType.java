package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

/**
 * Base class for {@link AbstractGeometryFieldMapper.AbstractGeometryFieldType}
 * and {@link GeoPointFieldMapper.GeoPointFieldType}
 */
public abstract class SearchableGeometryFieldType extends MappedFieldType {
    protected SearchableGeometryFieldType.QueryProcessor geometryQueryBuilder;

    protected SearchableGeometryFieldType() {
        setIndexOptions(IndexOptions.DOCS);
        setTokenized(false);
        setStored(false);
        setStoreTermVectors(false);
        setOmitNorms(true);
    }

    protected SearchableGeometryFieldType(SearchableGeometryFieldType ref) {
        super(ref);
    }

    /**
     * interface representing a query builder that generates a query from the given shape
     */
    public interface QueryProcessor {
        Query process(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context);

        @Deprecated
        default Query process(Geometry shape, String fieldName, SpatialStrategy strategy, ShapeRelation relation,
                              QueryShardContext context) {
            return process(shape, fieldName, relation, context);
        }
    }

    @Override
    public abstract SearchableGeometryFieldType clone();

    @Override
    public String typeName() {
        return GeoPointFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        throw new QueryShardException(context,
            "Geometry fields do not support exact searching, use dedicated geometry queries instead");
    }

    public void setGeometryQueryBuilder(SearchableGeometryFieldType.QueryProcessor geometryQueryBuilder)  {
        this.geometryQueryBuilder = geometryQueryBuilder;
    }

    public SearchableGeometryFieldType.QueryProcessor geometryQueryBuilder() {
        return geometryQueryBuilder;
    }
}
