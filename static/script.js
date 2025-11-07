// JavaScript para o Sistema de Precatórios

let modifiedData = {};
let originalData = {};
const SELECTED_STORAGE_KEY = 'selected_precatorios_ids';

// Inicialização
document.addEventListener('DOMContentLoaded', function() {
    // Remover loading se estiver ativo (caso tenha sido aplicado filtro)
    showLoading(false);
    
    initializeTable();
    setupSearch();
    setupEventListeners();
    syncSelectionFromStorage();
    setupDatalistClickable();
    reconfigureSearchableSelect = setupSearchableSelect();
    
    // Configurar atualização dinâmica dos filtros
    setupDynamicFilters();
    
    // Garantir que input hidden de organização está sincronizado com input visível
    const organizacaoInput = document.getElementById('filter_organizacao_input');
    const organizacaoHidden = document.getElementById('filter_organizacao_hidden');
    if (organizacaoInput && organizacaoHidden) {
        // Se o input visível tem valor mas o hidden não, sincronizar
        if (organizacaoInput.value && !organizacaoHidden.value) {
            // Tentar encontrar o valor nas opções
            const options = document.querySelectorAll('#organizacao_options .searchable-select-option');
            options.forEach(opt => {
                if (opt.textContent.trim() === organizacaoInput.value.trim()) {
                    organizacaoHidden.value = opt.dataset.value || organizacaoInput.value;
                }
            });
        }
        // Se o hidden tem valor mas o visível não, sincronizar
        if (organizacaoHidden.value && !organizacaoInput.value) {
            const options = document.querySelectorAll('#organizacao_options .searchable-select-option');
            options.forEach(opt => {
                if (opt.dataset.value === organizacaoHidden.value) {
                    organizacaoInput.value = opt.textContent;
                }
            });
        }
    }
});

// Inicializar tabela
function initializeTable() {
    const table = document.getElementById('dataTable');
    const rows = table.querySelectorAll('tbody tr');
    
    // Armazenar dados originais
    rows.forEach(row => {
        const precatorioId = row.dataset.precatorio;
        originalData[precatorioId] = {};
        
        const cells = row.querySelectorAll('td[data-field]');
        cells.forEach(cell => {
            const field = cell.dataset.field;
            const fieldElement = cell.querySelector('.editable-field');
            const originalValue = fieldElement ? fieldElement.value : cell.dataset.original;
            originalData[precatorioId][field] = originalValue;
        });
    });
    
    updateModifiedCount();
}

// Configurar busca (removido - agora usamos filtros do servidor)
function setupSearch() {
    // Busca local removida - agora usamos filtros do servidor
}

// Configurar event listeners
function setupEventListeners() {
    // Adicionar listeners para campos editáveis
    const editableFields = document.querySelectorAll('.editable-field');
    editableFields.forEach(field => {
        field.addEventListener('blur', function() {
            validateField(this);
        });
        
        field.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                this.blur();
            }
        });
    });
    
    // Adicionar listeners para cabeçalhos ordenáveis
    const sortableHeaders = document.querySelectorAll('.sortable-header');
    sortableHeaders.forEach(header => {
        header.addEventListener('click', function() {
            sortByField(this.dataset.field);
        });
    });
}

// ===== Seleção persistente entre páginas =====
function getStoredSelectedIds() {
    try {
        const raw = localStorage.getItem(SELECTED_STORAGE_KEY);
        const arr = raw ? JSON.parse(raw) : [];
        return Array.isArray(arr) ? arr : [];
    } catch (e) {
        return [];
    }
}

function setStoredSelectedIds(ids) {
    const unique = Array.from(new Set(ids.map(String)));
    localStorage.setItem(SELECTED_STORAGE_KEY, JSON.stringify(unique));
}

function addIdsToSelection(ids) {
    const current = getStoredSelectedIds();
    setStoredSelectedIds(current.concat(ids.map(String)));
    reflectSelectionOnPage();
}

function removeIdsFromSelection(ids) {
    const removeSet = new Set(ids.map(String));
    const current = getStoredSelectedIds();
    const next = current.filter(id => !removeSet.has(String(id)));
    setStoredSelectedIds(next);
    reflectSelectionOnPage();
}

function clearStoredSelection() {
    localStorage.removeItem(SELECTED_STORAGE_KEY);
    reflectSelectionOnPage();
}

function syncSelectionFromStorage() {
    reflectSelectionOnPage();
}

function reflectSelectionOnPage() {
    const selectedIds = new Set(getStoredSelectedIds().map(String));
    const checkboxes = document.querySelectorAll('.row-checkbox');
    checkboxes.forEach(cb => {
        cb.checked = selectedIds.has(String(cb.value));
    });

    // Atualiza o estado do checkbox mestre
    const selectAllCheckbox = document.getElementById('selectAll');
    const allCount = checkboxes.length;
    const checkedCount = Array.from(checkboxes).filter(cb => cb.checked).length;
    if (checkedCount === 0) {
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = false;
    } else if (checkedCount === allCount) {
        selectAllCheckbox.checked = true;
        selectAllCheckbox.indeterminate = false;
    } else {
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = true;
    }

    // Atualiza visual do modal (se aberto)
    if (document.getElementById('bulkEditModal')) {
        updateSelectedRecordsDisplay();
    }
}

// Marcar campo como modificado
function markAsModified(field) {
    const cell = field.closest('td');
    const row = field.closest('tr');
    const precatorioId = row.dataset.precatorio;
    const fieldName = cell.dataset.field;
    
    if (!modifiedData[precatorioId]) {
        modifiedData[precatorioId] = {};
    }
    
    const currentValue = field.value;
    const originalValue = originalData[precatorioId][fieldName];
    
    // Só marcar como modificado se o valor realmente mudou
    if (originalValue !== currentValue) {
        modifiedData[precatorioId][fieldName] = currentValue;
        field.classList.add('modified');
    } else {
        // Se voltou ao valor original, remover da lista de modificações
        delete modifiedData[precatorioId][fieldName];
        field.classList.remove('modified');
        
        // Se não há mais modificações neste precatório, remover o objeto
        if (Object.keys(modifiedData[precatorioId]).length === 0) {
            delete modifiedData[precatorioId];
        }
    }
    
    updateModifiedCount();
}

// Validar campo
function validateField(field) {
    const fieldType = field.closest('td').dataset.type;
    const value = field.value.trim();
    
    // Remover classes de erro anteriores
    field.classList.remove('error');
    
    // Validações específicas por tipo
    if (fieldType === 'numeric') {
        if (value && isNaN(parseFloat(value))) {
            field.classList.add('error');
            showAlert('Valor numérico inválido', 'warning');
            return false;
        }
    }
    
    return true;
}

// Atualizar contador de modificações
function updateModifiedCount() {
    const count = Object.keys(modifiedData).length;
    const countElement = document.getElementById('modifiedCount');
    
    if (count > 0) {
        countElement.textContent = `${count} alteração(ões)`;
        countElement.className = 'badge bg-warning';
    } else {
        countElement.textContent = '0 alterações';
        countElement.className = 'badge bg-info';
    }
}

// Salvar alterações
function saveChanges() {
    if (Object.keys(modifiedData).length === 0) {
        showAlert('Nenhuma alteração para salvar', 'info');
        return;
    }
    
    // Mostrar resumo das alterações
    showChangesSummary();
    
    // Mostrar modal de confirmação
    const modal = new bootstrap.Modal(document.getElementById('confirmModal'));
    modal.show();
}

// Mostrar resumo das alterações
function showChangesSummary() {
    const summaryContainer = document.getElementById('changesSummary');
    let summaryHTML = '<h6>Alterações a serem salvas:</h6>';
    
    Object.keys(modifiedData).forEach(precatorioId => {
        summaryHTML += `<div class="change-item">
            <span class="field-name">Precatório ${precatorioId}:</span>
        </div>`;
        
        Object.keys(modifiedData[precatorioId]).forEach(fieldName => {
            const oldValue = originalData[precatorioId][fieldName] || '';
            const newValue = modifiedData[precatorioId][fieldName];
            
            summaryHTML += `<div class="change-item">
                <span class="field-name">${fieldName}:</span>
                <span class="field-change">
                    <span class="old-value">${oldValue}</span> → 
                    <span class="new-value">${newValue}</span>
                </span>
            </div>`;
        });
    });
    
    summaryContainer.innerHTML = summaryHTML;
}

// Confirmar salvamento
function confirmSave() {
    console.log('=== FUNÇÃO confirmSave CHAMADA ===');
    console.log('modifiedData:', modifiedData);
    console.log('Tipo de modifiedData:', typeof modifiedData);
    console.log('Chaves de modifiedData:', Object.keys(modifiedData));
    
    showLoading(true);
    
    // Verificar se há dados válidos
    if (Object.keys(modifiedData).length === 0) {
        console.log('NENHUM DADO MODIFICADO - RETORNANDO');
        showLoading(false);
        showAlert('Nenhuma alteração detectada', 'warning');
        return;
    }
    
    console.log('=== DEBUG FRONTEND ===');
    console.log('Dados modificados:', modifiedData);
    console.log('URL da requisição:', '/update');
    console.log('Iniciando fetch...');
    
    fetch('/update', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            data: modifiedData
        })
    })
    .then(response => {
        console.log('Resposta recebida:', response);
        console.log('Status:', response.status);
        console.log('OK:', response.ok);
        return response.json();
    })
    .then(data => {
        console.log('Dados da resposta:', data);
        showLoading(false);
        
        if (data.success) {
            showAlert(data.message, 'success');
            modifiedData = {};
            updateModifiedCount();
            
            // Fechar modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('confirmModal'));
            modal.hide();
            
            // Recarregar dados após 2 segundos
            setTimeout(() => {
                refreshData();
            }, 2000);
        } else {
            showAlert(data.message, 'danger');
        }
    })
    .catch(error => {
        console.log('ERRO na requisição:', error);
        showLoading(false);
        showAlert('Erro ao salvar alterações: ' + error.message, 'danger');
    });
}

// Desfazer alterações
function undoChanges() {
    if (Object.keys(modifiedData).length === 0) {
        showAlert('Nenhuma alteração para desfazer', 'info');
        return;
    }
    
    // Restaurar valores originais
    Object.keys(modifiedData).forEach(precatorioId => {
        Object.keys(modifiedData[precatorioId]).forEach(fieldName => {
            const cell = document.querySelector(`tr[data-precatorio="${precatorioId}"] td[data-field="${fieldName}"]`);
            const field = cell.querySelector('.editable-field');
            
            if (field) {
                field.value = originalData[precatorioId][fieldName] || '';
                field.classList.remove('modified', 'error');
            }
        });
    });
    
    // Limpar dados modificados
    modifiedData = {};
    updateModifiedCount();
    
    showAlert('Alterações desfeitas com sucesso', 'success');
}

// Mostrar loading ao aplicar filtros
function showFilterLoading() {
    showLoading(true);
    // O loading será removido quando a página recarregar
    // Mas adicionamos um timeout de segurança
    setTimeout(() => {
        showLoading(false);
    }, 30000); // 30 segundos máximo
}

// Atualizar dados
// Mostrar alerta
function showAlert(message, type) {
    const alertContainer = document.getElementById('alertContainer');
    const alertId = 'alert-' + Date.now();
    
    const alertHTML = `
        <div id="${alertId}" class="alert alert-${type} alert-dismissible fade show" role="alert">
            <i class="fas fa-${getAlertIcon(type)} me-2"></i>
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    alertContainer.insertAdjacentHTML('beforeend', alertHTML);
    
    // Auto-remover após 5 segundos
    setTimeout(() => {
        const alert = document.getElementById(alertId);
        if (alert) {
            alert.remove();
        }
    }, 5000);
}

// Obter ícone do alerta
function getAlertIcon(type) {
    const icons = {
        'success': 'check-circle',
        'danger': 'exclamation-triangle',
        'warning': 'exclamation-circle',
        'info': 'info-circle'
    };
    return icons[type] || 'info-circle';
}

// Mostrar/esconder loading
function showLoading(show) {
    const overlay = document.getElementById('loadingOverlay');
    if (show) {
        overlay.classList.add('show');
    } else {
        overlay.classList.remove('show');
    }
}

// Atalhos de teclado
document.addEventListener('keydown', function(e) {
    // Ctrl+S para salvar
    if (e.ctrlKey && e.key === 's') {
        e.preventDefault();
        saveChanges();
    }
    
    // Ctrl+Z para desfazer
    if (e.ctrlKey && e.key === 'z') {
        e.preventDefault();
        undoChanges();
    }
    
    // F5 para atualizar
    if (e.key === 'F5') {
        e.preventDefault();
        refreshData();
    }
});

// Função para ordenar por campo
function sortByField(field) {
    const urlParams = new URLSearchParams(window.location.search);
    const currentSort = urlParams.get('sort') || 'ordem';
    const currentOrder = urlParams.get('order') || 'asc';
    
    // Determinar nova ordem
    let newOrder = 'asc';
    if (currentSort === field && currentOrder === 'asc') {
        newOrder = 'desc';
    }
    
    // Atualizar parâmetros da URL
    urlParams.set('sort', field);
    urlParams.set('order', newOrder);
    urlParams.set('page', '1'); // Voltar para primeira página
    
    // Redirecionar com novos parâmetros
    window.location.href = window.location.pathname + '?' + urlParams.toString();
}

// ===== FUNCIONALIDADES DE EDIÇÃO EM MASSA =====

// Abrir modal de edição em massa
function openBulkEditModal() {
    const modal = new bootstrap.Modal(document.getElementById('bulkEditModal'));
    updateSelectedRecordsDisplay();
    populateEditableFields();
    modal.show();
}

// Atualizar display dos registros selecionados
function updateSelectedRecordsDisplay() {
    const selectedIds = getStoredSelectedIds();
    const selectedRecordsDiv = document.getElementById('selectedRecords');
    const selectionInfoSpan = document.getElementById('selectionInfo');

    if (!selectedIds || selectedIds.length === 0) {
        selectedRecordsDiv.innerHTML = '<small class="text-muted">Nenhum registro selecionado</small>';
        if (selectionInfoSpan) {
            selectionInfoSpan.textContent = 'Selecione registros para edição em massa';
        }
        return;
    }

    let html = `<div class="mb-2"><strong>${selectedIds.length} registro(s) selecionado(s):</strong></div>`;

    // Mostra detalhado apenas para os visíveis na página atual
    const visibleById = {};
    document.querySelectorAll('tbody tr').forEach(row => {
        const id = row.getAttribute('data-precatorio');
        if (!id) return;
        const precatorioCell = row.querySelector('[data-field="precatorio"]');
        const organizacaoCell = row.querySelector('[data-field="organizacao"]');
        visibleById[id] = {
            precatorio: precatorioCell ? precatorioCell.textContent.trim() : id,
            organizacao: organizacaoCell ? organizacaoCell.textContent.trim() : 'N/A'
        };
    });

    selectedIds.forEach(id => {
        if (visibleById[id]) {
            html += `<div class="d-flex justify-content-between align-items-center mb-1">
                        <small>${visibleById[id].precatorio}</small>
                        <small class="text-muted">${visibleById[id].organizacao}</small>
                     </div>`;
        }
    });

    const hiddenCount = selectedIds.filter(id => !visibleById[id]).length;
    if (hiddenCount > 0) {
        html += `<div class="mt-2"><small class="text-muted">+ ${hiddenCount} selecionado(s) em outras páginas</small></div>`;
    }

    selectedRecordsDiv.innerHTML = html;
    
    // Atualizar informação de seleção
    if (selectionInfoSpan) {
        if (hiddenCount > 0) {
            selectionInfoSpan.textContent = `${selectedIds.length} registros selecionados (${selectedIds.length - hiddenCount} nesta página, ${hiddenCount} em outras páginas)`;
        } else {
            selectionInfoSpan.textContent = `${selectedIds.length} registros selecionados nesta página`;
        }
    }
}

// Popular campos editáveis no modal
function populateEditableFields() {
    const editableFieldsDiv = document.getElementById('editableFields');
    const editableFields = [
        { name: 'situacao', label: 'Situação', type: 'text' },
        { name: 'valor', label: 'Valor', type: 'text' }
    ];
    
    let html = '';
    editableFields.forEach(field => {
        html += `
            <div class="mb-3">
                <label class="form-label">${field.label}:</label>
                <input type="${field.type}" 
                       class="form-control form-control-sm" 
                       id="bulk_${field.name}" 
                       placeholder="Novo valor para ${field.label}">
                <small class="form-text text-muted">Deixe vazio para não alterar este campo</small>
            </div>
        `;
    });
    
    editableFieldsDiv.innerHTML = html;
}

// Selecionar todos os registros visíveis
function selectAllVisible() {
    const checkboxes = document.querySelectorAll('.row-checkbox');
    const ids = Array.from(checkboxes).map(cb => cb.value);
    addIdsToSelection(ids);
}

// Selecionar todas as páginas
function selectAllPages() {
    showLoading(true);
    
    // Buscar todos os IDs do banco
    fetch('/api/get_all_ids')
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            addIdsToSelection(data.ids);
            showAlert(`Selecionados ${data.ids.length} registros de todas as páginas`, 'success');
        } else {
            showAlert('Erro ao carregar IDs: ' + data.message, 'danger');
        }
    })
    .catch(error => {
        showAlert('Erro ao selecionar todas as páginas: ' + error.message, 'danger');
    })
    .finally(() => {
        showLoading(false);
    });
}

// Limpar seleção
function clearSelection() {
    clearStoredSelection();
}

// Toggle selecionar todos
function toggleSelectAll() {
    const selectAllCheckbox = document.getElementById('selectAll');
    const checkboxes = document.querySelectorAll('.row-checkbox');
    const ids = Array.from(checkboxes).map(cb => cb.value);
    if (selectAllCheckbox.checked) {
        addIdsToSelection(ids);
    } else {
        removeIdsFromSelection(ids);
    }
}

// Confirmar atualização em massa
function confirmBulkUpdate() {
    const selectedIds = getStoredSelectedIds();

    if (selectedIds.length === 0) {
        showAlert('Selecione pelo menos um registro para atualizar', 'warning');
        return;
    }
    
    // Coletar campos para atualizar
    const fieldUpdates = {};
    const editableFields = ['situacao', 'valor'];
    
    editableFields.forEach(field => {
        const input = document.getElementById(`bulk_${field}`);
        if (input && input.value.trim() !== '') {
            fieldUpdates[field] = input.value.trim();
        }
    });
    
    if (Object.keys(fieldUpdates).length === 0) {
        showAlert('Preencha pelo menos um campo para atualizar', 'warning');
        return;
    }
    
    // Mostrar loading
    showLoading(true);
    
    // Fazer requisição
    fetch('/bulk_update', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            selected_ids: selectedIds,
            field_updates: fieldUpdates
        })
    })
    .then(response => response.json())
    .then(data => {
        showLoading(false);
        
        if (data.success) {
            showAlert(data.message, 'success');
            
            // Fechar modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('bulkEditModal'));
            modal.hide();
            
            // Limpar seleção
            clearStoredSelection();
            
            // Recarregar dados após 2 segundos
            setTimeout(() => {
                refreshData();
            }, 2000);
        } else {
            showAlert(data.message, 'danger');
        }
    })
    .catch(error => {
        showLoading(false);
        showAlert('Erro ao atualizar registros: ' + error.message, 'danger');
    });
}

// Configurar event listeners para checkboxes
document.addEventListener('change', function(e) {
    if (e.target.classList.contains('row-checkbox')) {
        const id = e.target.value;
        if (e.target.checked) {
            addIdsToSelection([id]);
        } else {
            removeIdsFromSelection([id]);
        }
    }
});

// Confirmar antes de sair se há alterações não salvas
window.addEventListener('beforeunload', function(e) {
    if (Object.keys(modifiedData).length > 0) {
        e.preventDefault();
        e.returnValue = 'Há alterações não salvas. Deseja realmente sair?';
    }
});

// Configurar dropdown pesquisável para organização
function setupSearchableSelect() {
    const input = document.getElementById('filter_organizacao_input');
    const hidden = document.getElementById('filter_organizacao_hidden');
    const dropdown = document.getElementById('organizacao_dropdown');
    const searchInput = document.getElementById('organizacao_search');
    const options = document.getElementById('organizacao_options');
    
    if (!input || !dropdown || !options) return;
    
    // Flag para evitar duplo clique
    let isOpening = false;
    let optionsLoaded = false;
    
    // Função para carregar opções se necessário
    const ensureOptionsLoaded = () => {
        if (optionsLoaded) return Promise.resolve();
        
        // Verificar se já tem opções (mais que apenas "Todas as organizações")
        const existingOptions = options.querySelectorAll('.searchable-select-option');
        if (existingOptions.length > 1) {
            optionsLoaded = true;
            return Promise.resolve();
        }
        
        // Mostrar indicador de carregamento
        const loadingIndicator = document.createElement('div');
        loadingIndicator.className = 'searchable-select-option';
        loadingIndicator.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>Carregando organizações...';
        loadingIndicator.style.textAlign = 'center';
        loadingIndicator.style.color = '#6c757d';
        options.appendChild(loadingIndicator);
        
        return fetch('/api/get_filter_options?field=organizacao')
            .then(response => response.json())
            .then(data => {
                loadingIndicator.remove();
                if (data.success && data.values && data.values.length > 0) {
                    // Manter apenas a primeira opção (Todas as organizações)
                    const firstOption = options.querySelector('.searchable-select-option[data-value=""]');
                    options.innerHTML = '';
                    if (firstOption) options.appendChild(firstOption);
                    
                    // Adicionar todas as opções
                    data.values.forEach(value => {
                        const option = document.createElement('div');
                        option.className = 'searchable-select-option';
                        option.dataset.value = value;
                        option.textContent = value;
                        options.appendChild(option);
                    });
                    
                    // Reconfigurar eventos
                    if (reconfigureSearchableSelect) {
                        reconfigureSearchableSelect();
                    }
                    optionsLoaded = true;
                } else {
                    // Se não houver dados, mostrar mensagem
                    const noDataMsg = document.createElement('div');
                    noDataMsg.className = 'searchable-select-option';
                    noDataMsg.textContent = 'Nenhuma organização encontrada';
                    noDataMsg.style.textAlign = 'center';
                    noDataMsg.style.color = '#6c757d';
                    options.appendChild(noDataMsg);
                }
            })
            .catch(error => {
                loadingIndicator.remove();
                console.error('Erro ao carregar opções de organização:', error);
                const errorMsg = document.createElement('div');
                errorMsg.className = 'searchable-select-option';
                errorMsg.textContent = 'Erro ao carregar. Clique para tentar novamente.';
                errorMsg.style.textAlign = 'center';
                errorMsg.style.color = '#dc3545';
                options.appendChild(errorMsg);
                optionsLoaded = false; // Permitir tentar novamente
            });
    };
    
    // Limpar busca ao abrir dropdown
    const openDropdown = () => {
        if (isOpening) return; // Prevenir duplo clique
        isOpening = true;
        
        const isVisible = dropdown.style.display === 'flex';
        dropdown.style.display = isVisible ? 'none' : 'flex';
        
        if (!isVisible && searchInput) {
            // Carregar opções se necessário
            ensureOptionsLoaded().then(() => {
                searchInput.value = '';
                setTimeout(() => {
                    searchInput.focus();
                    // Limpar filtros ao abrir
                    const allOptions = options.querySelectorAll('.searchable-select-option');
                    allOptions.forEach(opt => opt.classList.remove('hidden'));
                    isOpening = false;
                }, 50);
            }).catch(() => {
                isOpening = false;
            });
        } else {
            isOpening = false;
        }
    };
    
    // Usar diretamente o elemento (não clonar para evitar problemas)
    const newInputElement = input;
    
    // Remover listeners antigos se existirem (usar AbortController para melhor controle)
    const controller = new AbortController();
    
    // Adicionar apenas um listener de clique (removido focus para evitar duplo clique)
    newInputElement.addEventListener('click', function(e) {
        e.stopPropagation();
        e.preventDefault();
        openDropdown();
    }, { signal: controller.signal });
    
    // Busca no dropdown - filtrar localmente (valores já carregados do servidor)
    let searchHandlerAdded = false;
    if (searchInput && !searchHandlerAdded) {
        searchInput.addEventListener('input', function() {
            const searchTerm = this.value.toLowerCase().trim();
            const allOptions = options.querySelectorAll('.searchable-select-option');
            allOptions.forEach(option => {
                const text = option.textContent.toLowerCase();
                if (text.includes(searchTerm)) {
                    option.classList.remove('hidden');
                } else {
                    option.classList.add('hidden');
                }
            });
        });
        searchHandlerAdded = true;
    }
    
    // Configurar seleção de opções (adicionar listeners novos)
    const setupOptionClick = () => {
        // Adicionar novos listeners
        const updatedOptions = options.querySelectorAll('.searchable-select-option');
        updatedOptions.forEach(option => {
            option.addEventListener('click', function() {
                const value = this.dataset.value;
                const text = this.textContent;
                
                newInputElement.value = value ? text : '';
                if (hidden) hidden.value = value;
                
                // Marcar como selecionada
                updatedOptions.forEach(opt => opt.classList.remove('selected'));
                this.classList.add('selected');
                
                dropdown.style.display = 'none';
                newInputElement.blur();
            });
        });
        
        // Marcar opção selecionada inicialmente
        const currentValue = newInputElement.value || (hidden ? hidden.value : '');
        updatedOptions.forEach(option => {
            if (option.dataset.value === currentValue || option.textContent === currentValue) {
                option.classList.add('selected');
            }
        });
    };
    
    setupOptionClick();
    
    // Fechar ao clicar fora (usar event delegation para melhor performance)
    let clickOutsideHandler = null;
    const setupClickOutside = () => {
        if (clickOutsideHandler) {
            document.removeEventListener('click', clickOutsideHandler);
        }
        clickOutsideHandler = function(e) {
            if (!newInputElement.contains(e.target) && !dropdown.contains(e.target)) {
                dropdown.style.display = 'none';
                isOpening = false;
            }
        };
        // Usar setTimeout para evitar conflito com o clique que abriu o dropdown
        setTimeout(() => {
            document.addEventListener('click', clickOutsideHandler);
        }, 100);
    };
    
    setupClickOutside();
    
    // Retornar função para reconfigurar após carregar opções
    return setupOptionClick;
}

// Variável global para reconfigurar após carregar opções
let reconfigureSearchableSelect = null;

// Tornar campos com datalist clicáveis para abrir o dropdown
function setupDatalistClickable() {
    const datalistInputs = document.querySelectorAll('input[list]');
    datalistInputs.forEach(input => {
        // Adicionar listener de clique no próprio input
        input.addEventListener('click', function(e) {
            const rect = this.getBoundingClientRect();
            const clickX = e.clientX - rect.left;
            const inputWidth = rect.width;
            
            // Se o clique foi na área do ícone (últimos 2.5rem à direita)
            const iconAreaWidth = 40; // 2.5rem = ~40px
            if (clickX >= inputWidth - iconAreaWidth) {
                e.preventDefault();
                this.focus();
                
                // Tentar abrir datalist simulando tecla ArrowDown
                setTimeout(() => {
                    const arrowDownEvent = new KeyboardEvent('keydown', {
                        key: 'ArrowDown',
                        code: 'ArrowDown',
                        keyCode: 40,
                        which: 40,
                        bubbles: true,
                        cancelable: true
                    });
                    this.dispatchEvent(arrowDownEvent);
                }, 50);
            }
        });
        
        // Também permitir clique duplo em qualquer lugar para abrir
        input.addEventListener('dblclick', function() {
            this.focus();
            setTimeout(() => {
                const arrowDownEvent = new KeyboardEvent('keydown', {
                    key: 'ArrowDown',
                    code: 'ArrowDown',
                    keyCode: 40,
                    which: 40,
                    bubbles: true,
                    cancelable: true
                });
                this.dispatchEvent(arrowDownEvent);
            }, 50);
        });
    });
}

// Função para carregar opções de dropdown via AJAX (sequencial para evitar sobrecarga)
function loadDropdownOptions(fields) {
    const queue = Array.from(fields);
    const loadNext = () => {
        const field = queue.shift();
        if (!field) return;
        fetch(`/api/get_filter_options?field=${field}`)
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // Para campos select normais
                    const select = document.querySelector(`select[name="filter_${field}"]`);
                    if (select) {
                        const firstOption = select.firstElementChild;
                        select.innerHTML = '';
                        if (firstOption) select.appendChild(firstOption);
                        data.values.forEach(value => {
                            const option = document.createElement('option');
                            option.value = value;
                            option.textContent = value;
                            select.appendChild(option);
                        });
                    }
                    
                    // Para dropdown pesquisável de organização
                    if (field === 'organizacao') {
                        const optionsContainer = document.getElementById('organizacao_options');
                        if (optionsContainer) {
                            const firstOption = optionsContainer.querySelector('.searchable-select-option[data-value=""]');
                            optionsContainer.innerHTML = '';
                            if (firstOption) optionsContainer.appendChild(firstOption);
                            data.values.forEach(value => {
                                const option = document.createElement('div');
                                option.className = 'searchable-select-option';
                                option.dataset.value = value;
                                option.textContent = value;
                                optionsContainer.appendChild(option);
                            });
                            // Reconfigurar eventos para as novas opções
                            if (reconfigureSearchableSelect) {
                                reconfigureSearchableSelect();
                            }
                        }
                    }
                }
            })
            .catch(error => {
                console.error(`Erro ao carregar opções para ${field}:`, error);
            })
            .finally(() => {
                // Intervalo menor entre requisições para melhor performance
                setTimeout(loadNext, 200);
            });
    };
    loadNext();
}

// Configurar filtros dinâmicos: organização mostra TODAS, outros filtros são dinâmicos baseados em TODOS os filtros ativos
function setupDynamicFilters() {
    const organizacaoInput = document.getElementById('filter_organizacao_hidden');
    const allFilterInputs = document.querySelectorAll('input[name^="filter_"], select[name^="filter_"]');
    const otherFilterSelects = document.querySelectorAll('select[name^="filter_"]:not([name="filter_organizacao"])');
    
    // Função para obter TODOS os filtros ativos
    function getAllActiveFilters() {
        const active = {};
        
        // Capturar todos os inputs e selects de filtro
        allFilterInputs.forEach(input => {
            const fieldName = input.name.replace('filter_', '');
            let value = '';
            
            if (input.type === 'hidden' || input.type === 'text' || input.type === 'number') {
                value = input.value ? input.value.trim() : '';
            } else if (input.tagName === 'SELECT') {
                value = input.value ? input.value.trim() : '';
            }
            
            if (value) {
                active[fieldName] = value;
            }
        });
        
        // Adicionar organização do input hidden (prioridade)
        if (organizacaoInput && organizacaoInput.value && organizacaoInput.value.trim()) {
            active.organizacao = organizacaoInput.value.trim();
        }
        
        // Também verificar o input visível de organização (caso o hidden não esteja preenchido)
        const organizacaoVisibleInput = document.getElementById('filter_organizacao_input');
        if (!active.organizacao && organizacaoVisibleInput && organizacaoVisibleInput.value && organizacaoVisibleInput.value.trim()) {
            active.organizacao = organizacaoVisibleInput.value.trim();
        }
        
        return active;
    }
    
    // Função para atualizar um filtro específico baseado em TODOS os filtros ativos
    function updateFilter(fieldName, skipField = null) {
        if (fieldName === skipField) return;
        
        const activeFilters = getAllActiveFilters();
        const select = document.querySelector(`select[name="filter_${fieldName}"]`);
        
        if (!select) return;
        
        const currentValue = select.value;
        
        // Construir URL com TODOS os filtros ativos (exceto o próprio campo)
        let url = `/api/get_filter_options?field=${fieldName}`;
        const filterCount = Object.keys(activeFilters).length;
        
        Object.keys(activeFilters).forEach(key => {
            if (key !== fieldName) {
                url += `&active_filter_${key}=${encodeURIComponent(activeFilters[key])}`;
            }
        });
        
        console.log(`Atualizando filtro ${fieldName} com ${filterCount} filtros ativos:`, activeFilters);
        
        // Mostrar loading
        const loadingOption = select.querySelector('.loading-option');
        if (!loadingOption) {
            const loading = document.createElement('option');
            loading.textContent = 'Atualizando...';
            loading.disabled = true;
            loading.className = 'loading-option';
            select.appendChild(loading);
        }
        
        fetch(url)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                const loading = select.querySelector('.loading-option');
                if (loading) loading.remove();
                
                // Limpar opções (exceto a primeira)
                const firstOption = select.firstElementChild;
                select.innerHTML = '';
                if (firstOption) select.appendChild(firstOption);
                
                if (data.success && data.values && data.values.length > 0) {
                    console.log(`Filtro ${fieldName} atualizado: ${data.values.length} valores encontrados`);
                    data.values.forEach(value => {
                        const option = document.createElement('option');
                        option.value = value;
                        option.textContent = value;
                        select.appendChild(option);
                    });
                    
                    // Restaurar valor selecionado se ainda existir
                    if (currentValue) {
                        const optionExists = Array.from(select.options).some(opt => opt.value === currentValue);
                        if (optionExists) {
                            select.value = currentValue;
                        } else {
                            select.value = ''; // Limpar se valor não existe mais
                            console.log(`Valor ${currentValue} não existe mais para ${fieldName}, limpo`);
                        }
                    }
                } else {
                    // Se não há valores, limpar seleção
                    console.warn(`Nenhum valor encontrado para ${fieldName} com os filtros ativos`);
                    select.value = '';
                }
            })
            .catch(error => {
                console.error(`Erro ao atualizar filtro ${fieldName}:`, error);
                const loading = select.querySelector('.loading-option');
                if (loading) loading.remove();
            });
    }
    
    // Função para atualizar TODOS os outros filtros baseado em filtros ativos
    function updateAllOtherFilters(changedField = null) {
        const filterFields = ['prioridade', 'tribunal', 'natureza', 'situacao', 'regime', 'ano_orc'];
        filterFields.forEach(field => {
            if (field !== changedField) {
                updateFilter(field, changedField);
            }
        });
    }
    
    // Verificar se há filtros pré-selecionados e atualizar outros filtros no carregamento
    function checkAndUpdatePreSelectedFilters() {
        const activeFilters = getAllActiveFilters();
        console.log('Verificando filtros pré-selecionados...', activeFilters);
        
        // Se há pelo menos um filtro ativo (especialmente organização), atualizar outros filtros
        if (Object.keys(activeFilters).length > 0) {
            console.log('Filtros pré-selecionados detectados, atualizando outros filtros...', activeFilters);
            // Atualizar todos os outros filtros baseado nos filtros ativos
            updateAllOtherFilters();
        } else {
            console.log('Nenhum filtro pré-selecionado encontrado');
        }
    }
    
    // Sincronizar input hidden com input visível de organização no carregamento
    function syncOrganizacaoInputs() {
        const organizacaoVisibleInput = document.getElementById('filter_organizacao_input');
        if (organizacaoVisibleInput && organizacaoVisibleInput.value && organizacaoInput) {
            // Se o input visível tem valor, tentar encontrar o valor correto no hidden
            const options = document.querySelectorAll('#organizacao_options .searchable-select-option');
            options.forEach(opt => {
                if (opt.textContent.trim() === organizacaoVisibleInput.value.trim()) {
                    organizacaoInput.value = opt.dataset.value || organizacaoVisibleInput.value;
                }
            });
            // Se não encontrou, usar o valor do input visível
            if (!organizacaoInput.value && organizacaoVisibleInput.value) {
                organizacaoInput.value = organizacaoVisibleInput.value;
            }
        }
    }
    
    // Atualizar filtros no carregamento inicial
    // Sincronizar primeiro, depois atualizar filtros
    setTimeout(() => {
        syncOrganizacaoInputs();
        checkAndUpdatePreSelectedFilters();
    }, 300);
    
    // Também verificar após um delay maior para garantir que tudo está carregado
    setTimeout(() => {
        syncOrganizacaoInputs();
        checkAndUpdatePreSelectedFilters();
    }, 1000);
    
    // Verificar uma última vez após tudo carregar (incluindo opções de organização)
    setTimeout(() => {
        syncOrganizacaoInputs();
        checkAndUpdatePreSelectedFilters();
    }, 2000);
    
    // Quando organização muda, atualizar outros filtros
    if (organizacaoInput) {
        let timeout;
        organizacaoInput.addEventListener('change', function() {
            clearTimeout(timeout);
            timeout = setTimeout(() => {
                updateAllOtherFilters('organizacao');
            }, 300); // Debounce de 300ms
        });
    }
    
    // Atualizar outros filtros quando qualquer filtro muda (cascata completa)
    otherFilterSelects.forEach(select => {
        let timeout;
        select.addEventListener('change', function() {
            clearTimeout(timeout);
            timeout = setTimeout(() => {
                const changedField = select.name.replace('filter_', '');
                updateAllOtherFilters(changedField);
            }, 300);
        });
    });
}

// Carregar opções com busca incremental no servidor (muito mais rápido)
function loadFilterOptionsWithSearch(fieldName, searchTerm = '') {
    // Para campos pequenos, não usar limite (carregar todos)
    // Para organização, usar limite maior
    const limit = fieldName === 'organizacao' ? 200 : null; // null = sem limite
    const limitParam = limit !== null ? `&limit=${limit}` : '';
    const url = `/api/get_filter_options?field=${fieldName}${limitParam}${searchTerm ? '&search=' + encodeURIComponent(searchTerm) : ''}`;
    
    const optionsContainer = document.getElementById('organizacao_options');
    if (optionsContainer) {
        // Mostrar loading
        const existingLoading = optionsContainer.querySelector('.loading-indicator');
        if (!existingLoading) {
            const loadingDiv = document.createElement('div');
            loadingDiv.className = 'searchable-select-option loading-indicator';
            loadingDiv.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>Buscando...';
            loadingDiv.style.textAlign = 'center';
            loadingDiv.style.color = '#6c757d';
            optionsContainer.appendChild(loadingDiv);
        }
    }
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000); // 5 segundos
    
    fetch(url, { signal: controller.signal })
        .then(response => {
            clearTimeout(timeoutId);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            return response.json();
        })
        .then(data => {
            const optionsContainer = document.getElementById('organizacao_options');
            if (optionsContainer) {
                const loading = optionsContainer.querySelector('.loading-indicator');
                if (loading) loading.remove();
                
                const firstOption = optionsContainer.querySelector('.searchable-select-option[data-value=""]');
                optionsContainer.innerHTML = '';
                if (firstOption) optionsContainer.appendChild(firstOption);
                
                if (data.success && data.values && data.values.length > 0) {
                    data.values.forEach(value => {
                        const option = document.createElement('div');
                        option.className = 'searchable-select-option';
                        option.dataset.value = value;
                        option.textContent = value;
                        optionsContainer.appendChild(option);
                    });
                } else {
                    const noResults = document.createElement('div');
                    noResults.className = 'searchable-select-option';
                    noResults.textContent = 'Nenhum resultado encontrado';
                    noResults.style.textAlign = 'center';
                    noResults.style.color = '#6c757d';
                    optionsContainer.appendChild(noResults);
                }
                
                if (reconfigureSearchableSelect) {
                    reconfigureSearchableSelect();
                }
            }
        })
        .catch(error => {
            clearTimeout(timeoutId);
            if (error.name !== 'AbortError') {
                console.error(`Erro ao buscar ${fieldName}:`, error);
            }
        });
}

// Carregar opção de filtro de forma assíncrona (com limite MUITO menor e retry)
function loadFilterOptionAsync(fieldName, selectElement = null) {
    // Verificar se já está carregado
    if (fieldName === 'organizacao') {
        const optionsContainer = document.getElementById('organizacao_options');
        if (optionsContainer && optionsContainer.querySelectorAll('.searchable-select-option').length > 1) {
            return Promise.resolve(); // Já carregado
        }
    } else {
        const select = selectElement || document.querySelector(`select[name="filter_${fieldName}"]`);
        if (select && select.options.length > 1) {
            return Promise.resolve(); // Já carregado
        }
    }
    
    // Para campos pequenos, carregar TODOS (sem limite)
    // Para organização, carregar 200 inicialmente
    const initialLimit = fieldName === 'organizacao' ? 200 : null; // null = sem limite
    
    // Mostrar indicador de carregamento
    if (fieldName === 'organizacao') {
        const optionsContainer = document.getElementById('organizacao_options');
        if (optionsContainer) {
            const loading = optionsContainer.querySelector('.loading-indicator');
            if (!loading) {
                const loadingDiv = document.createElement('div');
                loadingDiv.className = 'searchable-select-option loading-indicator';
                loadingDiv.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>Carregando...';
                loadingDiv.style.textAlign = 'center';
                loadingDiv.style.color = '#6c757d';
                optionsContainer.appendChild(loadingDiv);
            }
        }
    } else {
        const select = selectElement || document.querySelector(`select[name="filter_${fieldName}"]`);
        if (select) {
            const loadingOption = document.createElement('option');
            loadingOption.textContent = 'Carregando...';
            loadingOption.disabled = true;
            loadingOption.className = 'loading-option';
            select.appendChild(loadingOption);
        }
    }
    
    // Criar AbortController para timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 8000); // 8 segundos timeout
    
    // Construir URL com limite apenas se não for null
    const limitParam = initialLimit !== null ? `&limit=${initialLimit}` : '';
    const url = `/api/get_filter_options?field=${fieldName}${limitParam}`;
    
    return fetch(url, {
        signal: controller.signal
    })
        .then(response => {
            clearTimeout(timeoutId);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            // Remover indicador de loading
            if (fieldName === 'organizacao') {
                const optionsContainer = document.getElementById('organizacao_options');
                if (optionsContainer) {
                    const loading = optionsContainer.querySelector('.loading-indicator');
                    if (loading) loading.remove();
                }
            } else {
                const select = selectElement || document.querySelector(`select[name="filter_${fieldName}"]`);
                if (select) {
                    const loading = select.querySelector('.loading-option');
                    if (loading) loading.remove();
                }
            }
            
            if (data.success && data.values && data.values.length > 0) {
                if (fieldName === 'organizacao') {
                    // Atualizar dropdown pesquisável de organização
                    const optionsContainer = document.getElementById('organizacao_options');
                    if (optionsContainer) {
                        const firstOption = optionsContainer.querySelector('.searchable-select-option[data-value=""]');
                        optionsContainer.innerHTML = '';
                        if (firstOption) optionsContainer.appendChild(firstOption);
                        
                        data.values.forEach(value => {
                            const option = document.createElement('div');
                            option.className = 'searchable-select-option';
                            option.dataset.value = value;
                            option.textContent = value;
                            optionsContainer.appendChild(option);
                        });
                        
                        // Reconfigurar eventos
                        if (reconfigureSearchableSelect) {
                            reconfigureSearchableSelect();
                        }
                    }
                } else {
                    // Atualizar select normal
                    const select = selectElement || document.querySelector(`select[name="filter_${fieldName}"]`);
                    if (select) {
                        const firstOption = select.firstElementChild;
                        select.innerHTML = '';
                        if (firstOption) select.appendChild(firstOption);
                        
                        data.values.forEach(value => {
                            const option = document.createElement('option');
                            option.value = value;
                            option.textContent = value;
                            select.appendChild(option);
                        });
                    }
                }
            } else {
                // Se não retornou dados, mostrar mensagem
                showFilterError(fieldName, 'Nenhum dado disponível');
            }
        })
        .catch(error => {
            clearTimeout(timeoutId);
            console.error(`Erro ao carregar filtro ${fieldName}:`, error);
            
            // Remover indicador de loading
            if (fieldName === 'organizacao') {
                const optionsContainer = document.getElementById('organizacao_options');
                if (optionsContainer) {
                    const loading = optionsContainer.querySelector('.loading-indicator');
                    if (loading) loading.remove();
                }
            } else {
                const select = selectElement || document.querySelector(`select[name="filter_${fieldName}"]`);
                if (select) {
                    const loading = select.querySelector('.loading-option');
                    if (loading) loading.remove();
                }
            }
            
            // Mostrar erro e permitir retry
            showFilterError(fieldName, 'Erro ao carregar. Clique para tentar novamente.');
        });
}

// Mostrar erro em filtro
function showFilterError(fieldName, message) {
    if (fieldName === 'organizacao') {
        const optionsContainer = document.getElementById('organizacao_options');
        if (optionsContainer) {
            const errorDiv = document.createElement('div');
            errorDiv.className = 'searchable-select-option';
            errorDiv.textContent = message;
            errorDiv.style.textAlign = 'center';
            errorDiv.style.color = '#dc3545';
            errorDiv.style.cursor = 'pointer';
            errorDiv.onclick = () => {
                loadFilterOptionAsync(fieldName);
            };
            optionsContainer.appendChild(errorDiv);
        }
    } else {
        const select = document.querySelector(`select[name="filter_${fieldName}"]`);
        if (select) {
            const errorOption = document.createElement('option');
            errorOption.textContent = message;
            errorOption.disabled = true;
            errorOption.style.color = '#dc3545';
            select.appendChild(errorOption);
        }
    }
}

// Carregar opções de um único filtro
function loadSingleFilterOption(fieldName, selectElement) {
    // Verificar se já foi carregado
    if (selectElement.options.length > 1) {
        return;
    }
    
    // Marcar como carregando
    const loadingOption = document.createElement('option');
    loadingOption.textContent = 'Carregando...';
    loadingOption.disabled = true;
    selectElement.appendChild(loadingOption);
    
    fetch(`/api/get_filter_options?field=${fieldName}`)
        .then(response => response.json())
        .then(data => {
            loadingOption.remove();
            if (data.success && data.values && data.values.length > 0) {
                // Manter apenas a primeira opção (padrão)
                const firstOption = selectElement.firstElementChild;
                selectElement.innerHTML = '';
                if (firstOption) selectElement.appendChild(firstOption);
                
                // Adicionar todas as opções
                data.values.forEach(value => {
                    const option = document.createElement('option');
                    option.value = value;
                    option.textContent = value;
                    selectElement.appendChild(option);
                });
            }
        })
        .catch(error => {
            loadingOption.remove();
            console.error(`Erro ao carregar opções para ${fieldName}:`, error);
        });
}