package si.si.e7;

import org.apache.spark.sql.Row;
import scala.Tuple2;

public class RatioCB {
	private Integer idItem1;
	private Integer idItemTentation;
	private Integer similarity;

	public RatioCB() {
	};

	public RatioCB(Tuple2<Row, Integer> _x) {

		this.idItem1 = _x._1().getInt(0);
		this.idItemTentation = _x._1().getInt(0);
		this.similarity = _x._2();
	}

	public RatioCB(Integer idItem1, Integer idIntemTentation, Integer similarity) {

		this.idItem1 = idItem1;
		this.idItemTentation = idIntemTentation;
		this.similarity = similarity;
	}

	public Integer getIdItem1() {
		return idItem1;
	}

	public void setIdItem1(Integer idItem1) {
		this.idItem1 = idItem1;
	}

	public Integer getIdItemTentation() {
		return idItemTentation;
	}

	public void setIdItemTentation(Integer idItemTentation) {
		this.idItemTentation = idItemTentation;
	}

	public Integer getSimilarity() {
		return similarity;
	}

	public void setSimilarity(Integer similarity) {
		this.similarity = similarity;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((idItem1 == null) ? 0 : idItem1.hashCode());
		result = prime * result + ((idItemTentation == null) ? 0 : idItemTentation.hashCode());
		result = prime * result + ((similarity == null) ? 0 : similarity.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RatioCB other = (RatioCB) obj;
		if (idItem1 == null) {
			if (other.idItem1 != null)
				return false;
		} else if (!idItem1.equals(other.idItem1))
			return false;
		if (idItemTentation == null) {
			if (other.idItemTentation != null)
				return false;
		} else if (!idItemTentation.equals(other.idItemTentation))
			return false;
		if (similarity == null) {
			if (other.similarity != null)
				return false;
		} else if (!similarity.equals(other.similarity))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "RatioCB [idItem1=" + idItem1 + ", idItemTentation=" + idItemTentation + ", similarity=" + similarity
				+ "]";
	}

}
