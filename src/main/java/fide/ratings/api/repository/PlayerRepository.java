package fide.ratings.api.repository;

import java.util.List;
import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import fide.ratings.api.entity.Player;
import fide.ratings.api.enums.Countries;
import fide.ratings.api.enums.Flag;
import fide.ratings.api.enums.Gender;
import fide.ratings.api.enums.RatingTypes;
import fide.ratings.api.exception.PlayerNotFoundException;

@Repository
public class PlayerRepository {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public Player getPlayerByFideId(Long id) {
        String sql = "SELECT * FROM players WHERE fide_id = ?";
        
        try {
            return jdbcTemplate.queryForObject(sql, new PlayerRowMapper(), id);
        } catch (EmptyResultDataAccessException exception) {
            throw new PlayerNotFoundException();
        }
    }

    public List<Player> getPlayerByName(String name) {
        String sql = "SELECT fr.* FROM players fr JOIN player_names_fts fts ON fr.fide_id = fts.fide_id WHERE fts.name MATCH ?";
        return jdbcTemplate.query(sql, new PlayerRowMapper(), name);
    }

    public List<Player> getTopPlayers(int limit, Countries country, RatingTypes ratingType, Gender gender, Flag flag) {
        StringBuilder sql = new StringBuilder("SELECT * FROM players WHERE 1=1");
        List<Object> params = new ArrayList<>();

        // filter for country
        if (country != null) {
            sql.append(" AND country = ?");
            params.add(country.getCode());
        }

        // filter for gender
        if (gender != null) {
            sql.append(" AND sex = ?");
            params.add(gender.getCode());
        }

        // filter for flag
        if (flag != null) {
            String[] codes = flag.getCodes();
            sql.append(" AND (");
            for (int i = 0; i < codes.length; i++) {
                if (codes[i] == null) {
                    sql.append("flag IS NULL");
                } else {
                    sql.append("flag = ?");
                    params.add(codes[i]);
                }

                if (i < codes.length - 1) {
                    sql.append(" OR ");
                }
            }
            sql.append(")");
        }

        // add rating type filter
        if (ratingType != null) {
            sql.append(" ORDER BY " + ratingType.getCode() + " DESC LIMIT ?");
            params.add(limit);
        } else {
            sql.append(" ORDER BY rating DESC LIMIT ?"); // take rating as default
            params.add(limit);
        }
        return jdbcTemplate.query(sql.toString(), new PlayerRowMapper(), params.toArray());
    }
}
