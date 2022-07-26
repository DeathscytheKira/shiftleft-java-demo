/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telcel.app.web.siev.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.telcel.app.web.siev.dao.TtWsLlamadaRespuestaDAO;
import com.telcel.app.web.siev.entity.TtWsLlamadaRespuesta;
import com.telcel.app.web.siev.entity.jpa.TcAsignacionFuerza;
import com.telcel.app.web.siev.pojo.SievPojo;

/**
 *
 * @author ACI Group
 */
@Repository(value = "ttWsLlamadaRespuestaDAO")
public class JDBCTtWsLlamadaRespuesta implements TtWsLlamadaRespuestaDAO {

	private static final Logger log = LogManager.getLogger();

	private JdbcTemplate jdbcTemplate;

	private static final String SQL_INSERT_LLAMADO_RESPUESTA = "INSERT INTO TT_WS_LLAMADA_RESPUESTA ( ID,CLAVE_SISTEMA, LLAMADA, RESPUESTA, USUARIO, FECHA, METODO_INVOCADO, CAL_M200, CAL_ID_PROV, CAL_BURO, ID_TT_REGISTRO_EVALUACION,CLAVE_SIEV, NEMONICO_CC, CAL_FINANCIAMIENTO,FOLO_BURO_C,CUENTAS_M200, COND_ADICIONALES, TIEMPO_OPERACION, CAL_CAL_M200, TIPO_RENOVACION, TIPO_RENOVACION_DETALLE, ANTIGUEDAD) VALUES (SEQUENCE_TT_WS_LLAMADA_RESP.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static final String SQL_SELECT_EVALUACION_BY_REGISTRO_EVALUACION = "SELECT * FROM (SELECT * FROM TT_WS_LLAMADA_RESPUESTA WHERE ID_TT_REGISTRO_EVALUACION = :ID_TT_REGISTRO_EVALUACION ORDER BY ID DESC) WHERE ROWNUM=1";
	private static final String SQL_SELECT_CONSULTA_IPBC = "SELECT RESPUESTA_SIEV FROM TC_CONSULTA_IPBC WHERE DBMS_LOB.INSTR( (SELECT RESPUESTA FROM(SELECT * FROM TT_WS_LLAMADA_RESPUESTA WHERE ID_TT_REGISTRO_EVALUACION = :ID_TT_REGISTRO_EVALUACION ORDER BY ID DESC) WHERE ROWNUM=1),RESPUESTA_CC)>0";
	private static final String UPDATE_EVALUACION_BY_REGISTRO_EVALUACION = "UPDATE TT_WS_LLAMADA_RESPUESTA SET TIEMPO_OPERACION = :TIEMPO_OPERACION, CAL_FINANCIAMIENTO = :CAL_FINANCIAMIENTO, COND_ADICIONALES = :COND_ADICIONALES, RESULTADO_PORTABILIDAD = :RESULTADO_PORTABILIDAD, RESP_BURO_PORTA = :RESP_BURO_PORTA, RESP_TRAFICO_PORTA= :RESP_TRAFICO_PORTA WHERE  ID_TT_REGISTRO_EVALUACION = :ID_TT_REGISTRO_EVALUACION";
	private static final String UPDATE_LLAMADA_RESPUESTA_CONSULTADO = "UPDATE TT_WS_LLAMADA_RESPUESTA SET CONSULTA_EN = :CONSULTA_EN WHERE ID_TT_REGISTRO_EVALUACION = :ID_TT_REGISTRO_EVALUACION";

	private static final String SQL_AUX_PARAM_AA = "SELECT VALOR FROM AUX_PARAMS WHERE NOMBRE LIKE 'MAX_TIME_AA'";
	private String SQL_UPDATE_RESP_FINAN = "UPDATE " + " TT_WS_LLAMADA_RESPUESTA SET "
			+ "    CAL_FINANCIAMIENTO = ':CAL_FINANCIAMIENTO' " + " WHERE " + "    CLAVE_SIEV = ':CLAVE_SIEV'";
	private String SQL_UPDATE_COND_ADICIONALES = "UPDATE " + " TT_WS_LLAMADA_RESPUESTA SET "
			+ "    COND_ADICIONALES = ':COND_ADICIONALES' " + " WHERE " + "    CLAVE_SIEV = ':CLAVE_SIEV'";
	private String SQL_UPDATE_CLASIF_FINAN = "UPDATE " + " TT_WS_LLAMADA_RESPUESTA SET "
			+ "    CAL_CAL_M200 = ':CAL_CAL_M200' " + " WHERE " + "    CLAVE_SIEV = ':CLAVE_SIEV'";
	private String SQL_UPDATE_TIPO_RENOVACION_DETALLE = "UPDATE " + " TT_WS_LLAMADA_RESPUESTA SET "
			+ "    TIPO_RENOVACION_DETALLE = ':TIPO_RENOVACION_DETALLE' " + " WHERE "
			+ "    CLAVE_SIEV = ':CLAVE_SIEV'";
	private String SQL_UPDATE_ADEUDOS = "UPDATE " + " TT_WS_LLAMADA_RESPUESTA SET "
			+ "ADEUDO_ACTIVO = ':ADEUDO_ACTIVO' " + ", ADEUDO_INACTIVO = ':ADEUDO_INACTIVO' " + " WHERE "
			+ "CLAVE_SIEV = ':CLAVE_SIEV'";
	private String SQL_UPDATE_ERROR_CCS = "UPDATE " + " TT_WS_LLAMADA_RESPUESTA SET "
			+ "    CODIGO_ERROR_CSS = ':CODIGO_ERROR_CSS' " + ",    ERROR_MENSAJE_CCS = ':ERROR_MENSAJE_CCS' "
			+ " WHERE " + "CLAVE_SIEV = ':CLAVE_SIEV'";

	private String SQL_UPDATE_MOTIVO_AA = "UPDATE " + " TT_WS_LLAMADA_RESPUESTA SET " + "EVALUACION_MOTIVO = ':MOTIVO' "
			+ " WHERE " + "CLAVE_SIEV = ':CLAVE_SIEV'";

	private String SQL_UPDATE_MOTIVO_AA2 = "UPDATE TT_WS_LLAMADA_RESPUESTA SET EVALUACION_MOTIVO = ? "
			+ "WHERE CLAVE_SIEV = ?";

	private String SQL_UPDATE_CALIFICACIONES = "UPDATE " + " TT_WS_LLAMADA_RESPUESTA SET "
			+ "    CAL_M200 = ':CAL_M200' " + ",    CAL_ID_PROV = ':CAL_ID_PROV' " + ",    CAL_BURO = ':CAL_BURO' "
			+ " WHERE " + "    CLAVE_SIEV = ':CLAVE_SIEV'";

	private String SQL_SELECT_EXISTE_CSS = "SELECT ID FROM TT_WS_LLAMADA_RESPUESTA WHERE  CLAVE_SIEV = ':CLAVE_SIEV'";
	private String SQL_DELETE_EXISTE_CSS = "DELETE FROM tt_ws_llamada_respuesta WHERE ID = ':ID'";
	private String SQL_GET_CALFINAN = "SELECT CAL_CAL_M200 FROM TT_WS_LLAMADA_RESPUESTA WHERE  CLAVE_SIEV = ':CLAVE_SIEV'";
	private String SQL_GET_CALFINAL = "SELECT TIPO_CLIENTE FROM TT_REGISTRO_EVALUACION WHERE  FOLIO_SIEV = ':FOLIO_SIEV'";
	
	private String SQL_GET_RESPUESTA = "SELECT respuesta FROM TT_WS_LLAMADA_RESPUESTA WHERE ID = :ID";

	// metodo para buscar si hay una fuerza de venta autoasignada para un tiempo x
	private String SQL_SELECT_EXISTE_AA = "SELECT FECHA_CREACION,MOTIVO FROM TC_ASIGNACION_FUERZA WHERE V_NUM_EMPLEADO LIKE ':NEMP' ORDER BY ID FETCH NEXT 1 ROWS ONLY";

	@Autowired
	private DataSource dataSource;

	/**
	 * @return the jdbcTemplate
	 */
	public JdbcTemplate getJdbcTemplate() {
		if (jdbcTemplate == null) {
			jdbcTemplate = new JdbcTemplate(dataSource);
		}
		return jdbcTemplate;
	}

	/**
	 * @param jdbcTemplate the jdbcTemplate to set
	 */
	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public TtWsLlamadaRespuesta buscarPorClave(String id) {
		throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
																		// Tools | Templates.
	}

	@Override
	public TtWsLlamadaRespuesta buscarPorRegistroEvaluacion(int idTTRegistroEvaluacion) {
		String qry = SQL_SELECT_EVALUACION_BY_REGISTRO_EVALUACION.replace(":ID_TT_REGISTRO_EVALUACION",
				"'" + idTTRegistroEvaluacion + "'");
		log.info(">>>> qry: " + qry);
		TtWsLlamadaRespuesta ttWsLlamadaRespuesta = (TtWsLlamadaRespuesta) getJdbcTemplate().queryForObject(qry,
				new RowMapper<TtWsLlamadaRespuesta>() {
					@Override
					public TtWsLlamadaRespuesta mapRow(ResultSet rs, int arg1) throws SQLException {
						// Se rellena un bean Persona a partir de la fila actual
						// del ResultSet
						TtWsLlamadaRespuesta wsllR = new TtWsLlamadaRespuesta();
						wsllR.setId(rs.getInt("ID"));
						wsllR.setClaveSistema(rs.getString("CLAVE_SISTEMA"));
						wsllR.setUsuario(rs.getString("USUARIO"));
						wsllR.setFecha(rs.getDate("FECHA"));
						wsllR.setMetodoInvocado(rs.getString("METODO_INVOCADO"));
						wsllR.setClaveSiev(rs.getString("CLAVE_SIEV"));
						wsllR.setNemonicocc(rs.getString("NEMONICO_CC"));
						wsllR.setCalM200(rs.getString("CAL_M200"));
						wsllR.setCalIdProv(rs.getString("CAL_ID_PROV"));
						wsllR.setCalBuro(rs.getString("CAL_BURO"));
						wsllR.setCalFinanciamiento(rs.getString("CAL_FINANCIAMIENTO"));
						wsllR.setIdTTRegistroEvaluacion(rs.getInt("ID_TT_REGISTRO_EVALUACION"));
						wsllR.setFolioBuroc(rs.getString("FOLO_BURO_C"));
						wsllR.setCuentasM200(rs.getInt("CUENTAS_M200"));
						wsllR.setRespuesta(rs.getString("RESPUESTA"));
						wsllR.setCondAdicionales(rs.getString("COND_ADICIONALES"));
						wsllR.setTiempoOperacion(rs.getString("TIEMPO_OPERACION"));
						wsllR.setRespuestaPortabilidad(rs.getString("RESULTADO_PORTABILIDAD"));
						wsllR.setCalCalM2000(rs.getString("CAL_CAL_M200"));
						wsllR.setTipoRenovacionCred(rs.getInt("TIPO_RENOVACION"));
						wsllR.setTipoRenovacionDetalle(rs.getString("TIPO_RENOVACION_DETALLE"));
						wsllR.setAntiguedad(rs.getInt("ANTIGUEDAD"));
						wsllR.setAdeudoActivo(rs.getString("ADEUDO_ACTIVO"));
						wsllR.setAdeudoInactivo(rs.getString("ADEUDO_INACTIVO"));
						wsllR.setMensajeErrorCss(rs.getString("ERROR_MENSAJE_CCS"));
						wsllR.setCodigoErrorCss(rs.getString("CODIGO_ERROR_CSS"));
						wsllR.setEvaluacionMotivo(rs.getString("EVALUACION_MOTIVO"));
						return wsllR;
					}
				});
		log.info("query " + ttWsLlamadaRespuesta.getRespuesta());
		ttWsLlamadaRespuesta.setRespuestaCompleta(ttWsLlamadaRespuesta.getRespuesta());
		return ttWsLlamadaRespuesta;
	}
	
	@Override
	public TtWsLlamadaRespuesta respuestaById(int id) {
		String qry = SQL_GET_RESPUESTA.replace(":ID",
				"" + id);
		TtWsLlamadaRespuesta ttWsLlamadaRespuesta = (TtWsLlamadaRespuesta) getJdbcTemplate().queryForObject(qry,
				new RowMapper<TtWsLlamadaRespuesta>() {
					@Override
					public TtWsLlamadaRespuesta mapRow(ResultSet rs, int arg1) throws SQLException {
						// Se rellena
						TtWsLlamadaRespuesta wsllR = new TtWsLlamadaRespuesta();
						wsllR.setRespuesta(rs.getString("respuesta"));
						return wsllR;
					}
				});
		ttWsLlamadaRespuesta.setRespuestaCompleta(ttWsLlamadaRespuesta.getRespuesta());
		return ttWsLlamadaRespuesta;
	}

	@Override
	public List<TtWsLlamadaRespuesta> buscarTodos() {
		throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
																		// Tools | Templates.
	}

	@Override
	public void insertar(TtWsLlamadaRespuesta llamadaRespuesta) {

		jdbcTemplate.update(SQL_INSERT_LLAMADO_RESPUESTA, llamadaRespuesta.getClaveSistema(),
				llamadaRespuesta.getLlamada(), llamadaRespuesta.getRespuesta(), llamadaRespuesta.getUsuario(),
				Calendar.getInstance().getTime(), llamadaRespuesta.getMetodoInvocado(), llamadaRespuesta.getCalM200(),
				llamadaRespuesta.getCalIdProv(), llamadaRespuesta.getCalBuro(),
				llamadaRespuesta.getIdTTRegistroEvaluacion(), llamadaRespuesta.getClaveSiev(),
				llamadaRespuesta.getNemonicocc(), llamadaRespuesta.getCalFinanciamiento(),
				llamadaRespuesta.getFolioBuroc(), llamadaRespuesta.getCuentasM200(),
				llamadaRespuesta.getCondAdicionales(), llamadaRespuesta.getTiempoOperacion(),
				llamadaRespuesta.getCalCalM2000(), llamadaRespuesta.getTipoRenovacionCred(),
				llamadaRespuesta.getTipoRenovacionDetalle(), llamadaRespuesta.getAntiguedad());

		String query = SQL_SELECT_EXISTE_AA.replace(":NEMP", llamadaRespuesta.getUsuario());
		TcAsignacionFuerza tTcAsignacionFuerza = null;
		try {
			tTcAsignacionFuerza = (TcAsignacionFuerza) getJdbcTemplate().queryForObject(query,
					new RowMapper<TcAsignacionFuerza>() {
						@Override
						public TcAsignacionFuerza mapRow(ResultSet rs, int arg1) throws SQLException {
							TcAsignacionFuerza registro = new TcAsignacionFuerza();
							registro.setFechaCreacion(rs.getTimestamp("FECHA_CREACION"));
							registro.setMotivo(rs.getString("MOTIVO"));
							return registro;
						}
					});
		} catch (Exception e) {
		}
		if (tTcAsignacionFuerza != null) {
			long valorTime = 60;
			String query2 = SQL_AUX_PARAM_AA;
			try {
				valorTime = (long) getJdbcTemplate().queryForObject(query2, Long.class);
			} catch (Exception e) {
				valorTime = 60;
				log.error(e);
			}

			try {
				SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
				String sD1 = sdf.format(tTcAsignacionFuerza.getFechaCreacion());
				if (sD1.contains("."))
					sD1 = sD1.substring(0, sD1.indexOf("."));
				Date d1 = sdf.parse(sD1);
				Date d2 = new Date();

				long diffInMillies = Math.abs(d2.getTime() - d1.getTime());
				long diff = TimeUnit.MINUTES.convert(diffInMillies, TimeUnit.MILLISECONDS);

				if (diff < valorTime) {
					updateMotivoAA(llamadaRespuesta.getClaveSiev(), tTcAsignacionFuerza.getMotivo());
				}

			} catch (Exception e) {
				log.error(e);
			}
		}
	}

	@Override
	public void actualizar(TtWsLlamadaRespuesta objeto) {
		throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
																		// Tools | Templates.
	}

	@Override
	public void borrar(TtWsLlamadaRespuesta objeto) {
		throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
																		// Tools | Templates.
	}

	@Override
	public void updateFinanciamiento(SievPojo sp) {
		String query = UPDATE_EVALUACION_BY_REGISTRO_EVALUACION.replace(":TIEMPO_OPERACION",
				"'" + sp.getTipoActivacion() + "'");
		query = query.replace(":COND_ADICIONALES", "'" + sp.getCondicionesAdicionales() + "'");
		query = query.replace(":CAL_FINANCIAMIENTO", "'" + sp.getRespFinanciamiento() + "'");
		query = query.replace(":RESULTADO_PORTABILIDAD", "'" + sp.getResultadoPortabilidad() + "'");
		query = query.replace(":RESP_BURO_PORTA", "'" + sp.getRespBuroPorta() + "'");
		query = query.replace(":RESP_TRAFICO_PORTA", "'" + sp.getRespTraficoPorta() + "'");
		query = query.replace(":ID_TT_REGISTRO_EVALUACION", "'" + sp.getIdSiev() + "'");

		log.info("-- Query --> " + query);
		getJdbcTemplate().update(query);
	}

	@Override
	public void updateConsultaEn(String consultadoEn, String idSiev) {
		String query = UPDATE_LLAMADA_RESPUESTA_CONSULTADO.replace(":CONSULTA_EN", "'" + consultadoEn + "'");
		query = query.replace(":ID_TT_REGISTRO_EVALUACION", "'" + idSiev + "'");
		log.info("-- Query --> " + query);
		getJdbcTemplate().update(query);
	}

	@Override
	public void updateRespFinanciamiento(String folioFiev, String respFinan) {
		log.info(">>Resp Fianciamiento: " + respFinan + "<");
		String query = SQL_UPDATE_RESP_FINAN.replace(":CAL_FINANCIAMIENTO", respFinan);
		query = query.replace(":CLAVE_SIEV", folioFiev);
		log.info(query);
		getJdbcTemplate().update(query);
	}

	@Override
	public void updateCondicionesAdicionales(String folioFiev, String condicionAdicional) {
		log.info(">>Condicion adicional:" + condicionAdicional + "<");
		String query = SQL_UPDATE_COND_ADICIONALES.replace(":COND_ADICIONALES", condicionAdicional);
		query = query.replace(":CLAVE_SIEV", folioFiev);
		log.info(query);
		getJdbcTemplate().update(query);
	}

	@Override
	public void updateCalFinanciamineto(String folioFiev, String calFinan) {
		log.info(">>El clasif financiamiento es:" + calFinan + "<");
		String query = SQL_UPDATE_CLASIF_FINAN.replace(":CAL_CAL_M200", calFinan);
		query = query.replace(":CLAVE_SIEV", folioFiev);
		log.info(query);
		getJdbcTemplate().update(query);
	}

	@Override
	public void updateRenovacionDetalle(String folioFiev, String renoDetalle) {
		log.info(">>Tipo renovación detalle:" + renoDetalle + "<");
		String query = SQL_UPDATE_TIPO_RENOVACION_DETALLE.replace(":TIPO_RENOVACION_DETALLE", renoDetalle);
		query = query.replace(":CLAVE_SIEV", folioFiev);
		log.info(query);
		getJdbcTemplate().update(query);
	}

	@Override
	public void updateAdeudos(String folioFiev, String adeudoActivo, String adeudoInactivo) {
		String query = SQL_UPDATE_ADEUDOS.replace(":ADEUDO_ACTIVO", adeudoActivo);
		query = query.replace(":ADEUDO_INACTIVO", adeudoInactivo);
		query = query.replace(":CLAVE_SIEV", folioFiev);
		log.info(query);
		getJdbcTemplate().update(query);
	}

	@Override
	public void updateErrorCCS(String folioFiev, String codigoError, String mensajeError) {
		String query = SQL_UPDATE_ERROR_CCS.replace(":CODIGO_ERROR_CSS", codigoError);
		query = query.replace(":ERROR_MENSAJE_CCS", mensajeError);
		query = query.replace(":CLAVE_SIEV", folioFiev);
		log.info(query);
		getJdbcTemplate().update(query);
	}

	@Override
	public int existeCCS(String folioFiev) {
		int valor = 0;
		String query = SQL_SELECT_EXISTE_CSS.replace(":CLAVE_SIEV", folioFiev);
		try {
			valor = (Integer) getJdbcTemplate().queryForObject(query, Integer.class);
		} catch (Exception e) {
			valor = 0;
		}

		return valor;
	}

	@Override
	public void deleteRowCCS(int idCCS) {
		String valor = Integer.toString(idCCS);
		String query = SQL_DELETE_EXISTE_CSS.replace(":ID", valor.trim());
		log.info("Llamado a CCS, por reintento elemento a eliminar:" + query);
		getJdbcTemplate().update(query);
	}

	@Override
	public void updateCalificaciones(String folioFiev, String anacr, String provider, String buro) {
		String query = SQL_UPDATE_CALIFICACIONES.replace(":CAL_M200", anacr);
		query = query.replace(":CAL_ID_PROV", provider);
		query = query.replace(":CAL_BURO", buro);
		query = query.replace(":CLAVE_SIEV", folioFiev);
		log.info(query);
		getJdbcTemplate().update(query);
	}

	@Override
	public String getCalFinan(String folioFiev) {
		String valor = "";
		String query = SQL_GET_CALFINAN.replace(":CLAVE_SIEV", folioFiev);
		log.info(query);
		try {
			valor = (String) getJdbcTemplate().queryForObject(query, String.class);
		} catch (Exception e) {
			valor = "";
		}

		return valor;
	}

	@Override
	public String getCalFinal(String folioFiev) {
		String valor = "";
		String query = SQL_GET_CALFINAL.replace(":FOLIO_SIEV", folioFiev);
		log.info(query);
		try {
			valor = (String) getJdbcTemplate().queryForObject(query, String.class);
		} catch (Exception e) {
			valor = "";
		}

		return valor;
	}

	/*
	 * metodo para asignar motivo si es que tiene una fuerza de venta autoasiganda
	 */
	@Override
	public void updateMotivoAA(String folioFiev, String motivo) {

		/*
		 * String query = SQL_UPDATE_MOTIVO_AA.replace(":MOTIVO",
		 * motivo).replace(":CLAVE_SIEV", folioFiev); log.info(query);
		 * getJdbcTemplate().update(query);
		 */

		Object[] params = { motivo, folioFiev };
		int[] types = { Types.VARCHAR, Types.VARCHAR };

		int rows = getJdbcTemplate().update(SQL_UPDATE_MOTIVO_AA2, params, types);
		log.info(SQL_UPDATE_MOTIVO_AA2 + "---->" + rows + " row(s) updated.");
	}
}
